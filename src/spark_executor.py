#!/usr/bin/env python3

import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
import os
import time
import psutil
import traceback
import pyspark.sql.functions
import pyspark.sql.types
import pyspark.sql.window
from .logger import agent_logger # Import agent_logger

logger = logging.getLogger(__name__)

@dataclass
class ExecutionResult:
    """Represents the result of a Spark code execution"""
    success: bool
    data: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    execution_time: float = 0.0
    memory_used: str = "0MB"
    rows_processed: int = 0
    is_valid: bool = False

class SparkExecutor:
    """
    Handles the execution of generated Spark code in a controlled environment.
    Ensures read-only access to PostgreSQL and proper resource management.
    """
    def __init__(self):
        self._initialize_spark()
        agent_logger.info("SparkExecutor initialized", tags=["spark", "init"])
        # Define connection parameters used by read_table
        self.postgres_params = {
            "url": f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'localhost')}:"
                   f"{os.environ.get('POSTGRES_PORT', '5432')}/{os.environ.get('POSTGRES_DB')}",
            "properties": {
                "user": os.environ.get("POSTGRES_USER"),
                "password": os.environ.get("POSTGRES_PASSWORD"),
                "driver": "org.postgresql.Driver",
                "readonly": "true"
            }
        }
    
    def _initialize_spark(self):
        """Initialize the Spark session with proper configurations"""
        try:
            self.spark = SparkSession.builder\
                .appName("DataTransformation")\
                .config("spark.jars", "postgresql-42.2.5.jar")\
                .config("spark.executor.memory", "2g")\
                .config("spark.driver.memory", "2g")\
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
                .getOrCreate()
            
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize Spark session", exc_info=True)
            raise RuntimeError("Spark initialization failed") from e

    def read_table(self, qualified_table_name: str) -> DataFrame:
        """Read a table from PostgreSQL using the configured Spark session.
           Expects a schema-qualified table name (e.g., 'app_data.customers').
           NO LONGER TRACED.
        """
        try:
            logger.info(f"Attempting to read table: {qualified_table_name} using JDBC")
            read_properties = self.postgres_params["properties"].copy()

            result_df = self.spark.read.jdbc(
                url=self.postgres_params["url"],
                table=qualified_table_name, 
                properties=read_properties
            )
            return result_df
        except Exception as e:
            logger.error(f"Failed to read table {qualified_table_name}", exc_info=True)
            raise RuntimeError(f"Could not read table {qualified_table_name}") from e

    def execute(self, code: str, validation_checks: list[str]) -> tuple[bool, DataFrame | None, str]:
        """
        Execute the provided Spark code string and run validation checks.

        Args:
            code: The Spark code string to execute.
            validation_checks: A list of Python code strings for validation.

        Returns:
            A tuple: (is_valid, result_dataframe, error_message)
        """
        local_ns = {}
        error_message = ""
        result_df: DataFrame | None = None
        
        # Set up the execution context (globals)
        execution_globals = globals().copy()
        execution_globals.update({
            'read_table': self.read_table,
            'spark': self.spark,
            'pyspark': pyspark, # Provide the top-level pyspark module
            'pd': pd, # Ensure pandas is available
            'os': os # Ensure os is available
        })

        # === Step 1: Deterministic Syntax Check ===
        try:
            # Attempt to compile the code string first.
            # Raises SyntaxError if the Python syntax is invalid.
            compile(code, "<generated_code_string>", "exec")
            agent_logger.info("Generated code passed Python syntax check.", tags=["spark", "execution", "syntax_check"])
        except SyntaxError as se:
            # Prefix the error message to indicate a syntax error
            error_message = f"SYNTAX_ERROR:::Generated code failed Python syntax check: {se}"
            logger.error(error_message, exc_info=True)
            # Include traceback details specific to syntax error
            error_message += f"\nTraceback (SyntaxError):\n{traceback.format_exception_only(type(se), se)[-1]}"
            return False, None, error_message # Return failure immediately

        # === Step 2: Execute the code if syntax is valid ===
        logger.info(f"Executing Spark code with {len(validation_checks)} validation checks")
        try:
            # Execute the main transformation code
            exec(code, execution_globals, local_ns)
            result_df = local_ns.get('result')
            execution_error = local_ns.get('execution_error') # Check if Claude caught an error

            # Prioritize error caught by generated code over missing result
            if execution_error:
                 error_message = f"Error caught within generated code: {execution_error}"
                 logger.error(error_message)
                 # Treat as execution failure, skip validation
                 return False, None, error_message 
                 
            if result_df is None:
                # If no explicit error caught, but result is missing, raise this
                raise ValueError("Code executed without error but did not produce a Spark DataFrame named 'result'")
            if not isinstance(result_df, DataFrame):
                 raise TypeError(f"Variable 'result' was produced, but it is type {type(result_df).__name__}, not Spark DataFrame.")

            # Execute validation checks
            # Pass the df into the validation context
            validation_globals = execution_globals.copy()
            validation_locals = {'df': result_df} # Define df in the local scope for the check
            
            for check in validation_checks:
                try:
                    # The check string itself can now assume 'df' is defined
                    exec(check, validation_globals, validation_locals)
                except AssertionError as ae:
                    error_message = f"Validation failed: {check} -> {ae}"
                    logger.warning(error_message)
                    # Add the failed check detail to the error message for clarity
                    error_message += f" (Check code: {check})"
                    return False, result_df, error_message
                except Exception as ve:
                    error_message = f"Error during validation check '{check}': {ve}"
                    logger.error(error_message, exc_info=True)
                    # Add the failed check detail to the error message for clarity
                    error_message += f" (Check code: {check})"
                    return False, result_df, error_message

            logger.info("Spark code executed successfully and passed validation.")
            return True, result_df, ""

        except Exception as e:
            # This catches errors during exec() itself OR errors raised above
            error_message = f"Error executing/validating Spark code: {e}"
            logger.error(error_message, exc_info=True)
            error_message += f"\nTraceback:\n{traceback.format_exc()}"
            # Try to return the partial result_df if it exists from local_ns, otherwise None
            partial_result_df = local_ns.get('result')
            if not isinstance(partial_result_df, DataFrame):
                 partial_result_df = None
            return False, partial_result_df, error_message

    def stop(self):
        """Stop the Spark session."""
        if hasattr(self, 'spark') and self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

    # Example usage (for testing)
    # def sample_execution(self):
    #     code = """
    # from pyspark.sql.functions import col
    # df = read_table('customers')
    # result = df.filter(col('id') > 1).select('name', 'email')
    # """
    #     checks = [
    #         "assert 'name' in df.columns",
    #         "assert df.count() > 0"
    #     ]
    #     is_valid, df_result, error = self.execute(code, checks)
    #     if is_valid and df_result:
    #         df_result.show()
    #     else:
    #         print(f"Execution failed: {error}")

# Create a singleton instance for global use
executor = SparkExecutor() 