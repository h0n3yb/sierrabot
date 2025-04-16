#!/usr/bin/env python3

"""
Example script demonstrating the usage of the autonomous data transformation agent.
This script shows how to:
1. Generate Spark code from natural language
2. Execute the generated code
3. Handle the results and validation
"""

import logging
import os
from dotenv import load_dotenv
from src.code_generator import generator
from src.spark_executor import executor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def run_transformation(natural_language_request: str) -> None:
    """
    Executes a data transformation based on a natural language request.
    
    Args:
        natural_language_request: The transformation request in plain English
    """
    try:
        # Step 1: Generate Spark code
        logger.info("Generating code for request: %s", natural_language_request)
        generated = generator.generate(natural_language_request)
        
        logger.info("Generated code description: %s", generated.description)
        logger.info("Tables used: %s", ", ".join(generated.tables_used))
        logger.info("Estimated complexity: %s", generated.estimated_complexity)
        
        # Step 2: Execute the generated code
        logger.info("Executing generated code...")
        result = executor.execute(generated.code, generated.validation_checks)
        
        # Step 3: Handle the results
        if result.success and result.is_valid:
            logger.info("Transformation successful!")
            logger.info("Execution time: %.2f seconds", result.execution_time)
            logger.info("Memory used: %s", result.memory_used)
            logger.info("Rows processed: %d", result.rows_processed)
            
            # Display first few rows of the result
            if result.data is not None:
                print("\nFirst 5 rows of the result:")
                print(result.data.head())
            else:
                logger.warning("No data in result")
        else:
            if not result.success:
                logger.error("Transformation failed: %s", result.error)
            if not result.is_valid:
                logger.error("Result validation failed")
    
    except Exception as e:
        logger.error("Error during transformation", exc_info=True)
        raise

def main():
    """
    Demonstrates various transformation examples
    """
    # Example 1: Simple customer query
    print("\n=== Example 1: Simple Customer Query ===")
    run_transformation(
        "Get the names and emails of all customers who joined in 2024"
    )
    
    # Example 2: Sales analysis
    print("\n=== Example 2: Sales Analysis ===")
    run_transformation(
        "Calculate the total revenue and number of orders per product category, "
        "sorted by revenue in descending order"
    )
    
    # Example 3: Complex customer analysis
    print("\n=== Example 3: Complex Customer Analysis ===")
    run_transformation(
        "Find customers who have spent more than $1000 in total and have at least "
        "2 support tickets marked as 'Resolved', show their name, total spent, "
        "and number of resolved tickets"
    )
    
    # Example 4: Time-based analysis
    print("\n=== Example 4: Time-based Analysis ===")
    run_transformation(
        "Calculate the average time to resolve support tickets (in days) for each "
        "customer, only including customers with at least 3 resolved tickets"
    )

if __name__ == "__main__":
    main() 