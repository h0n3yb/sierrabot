#!/usr/bin/env python3

import logging
from typing import Dict, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import google.generativeai as genai
from dotenv import load_dotenv
import os
import ast
from .logger import agent_logger

# Load environment variables
load_dotenv()

# Configure Gemini
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

logger = logging.getLogger(__name__)

class CorrectionStrategy(Enum):
    """Types of correction strategies that can be applied"""
    RETRY_AS_IS = "retry_as_is"
    MODIFY_CODE = "modify_code"
    REGENERATE = "regenerate"
    ASK_USER = "ask_user"

@dataclass
class CorrectionResult:
    """Result of a correction attempt"""
    strategy: CorrectionStrategy
    success: bool
    modified_code: Optional[str] = None
    explanation: Optional[str] = None
    user_prompt: Optional[str] = None
    analysis: Optional[str] = None

class SelfCorrection:
    """
    Handles error analysis and correction attempts using Gemini Pro
    for reasoning about failures and determining correction strategies.
    """
    def __init__(self):
        self.model = genai.GenerativeModel('gemini-2.5-pro-exp-03-25')
        agent_logger.info("Initialized SelfCorrection module with Gemini 2.5 Pro", tags=["corrector", "init"])
    
    def attempt_correction(
        self,
        original_request: str,
        failed_code: str,
        failure_data: Dict[str, Any],
        execution_context: Dict
    ) -> CorrectionResult:
        """
        Analyzes a failed transformation and always recommends regeneration.
        """
        error_message = failure_data.get("error", "Unknown execution error")
        validation_failed = "Validation failed:" in error_message
        final_result: Optional[CorrectionResult] = None
        analysis_text = "Error analysis failed or was skipped."
        
        # Start analysis trace outside the main try block
        analysis_trace = agent_logger.start_operation(
            "gemini_error_analysis",
            metadata={
                "original_request": original_request,
                "failed_code_snippet": failed_code[:500] + "..." if len(failed_code) > 500 else failed_code, # Avoid overly long code
                "error_message": error_message,
                "validation_failed": validation_failed,
            },
            tags=["llm", "analysis", "gemini", "corrector"]
        )

        try:
            # === Step 1: Analyze Error with Gemini (NOW TRACED) ===
            try:
                analysis_prompt = self._construct_analysis_prompt(
                    original_request=original_request,
                    failed_code=failed_code,
                    error_message=error_message,
                    validation_failed=validation_failed,
                    schema_context=execution_context.get("schema_context", "Schema context not provided.")
                )
                # Use standard logger for info
                logger.info("Sending analysis prompt to Gemini. Length: %d", len(analysis_prompt))

                analysis_response = self.model.generate_content(analysis_prompt)
                analysis_text = analysis_response.text
                logger.info("Error analysis completed. Proceeding with REGENERATE strategy.")
                # End trace successfully
                agent_logger.end_operation(
                    analysis_trace, 
                    status="completed", 
                    result={"analysis_text_length": len(analysis_text)}
                )
            except Exception as analysis_e:
                analysis_text = f"Error during analysis: {analysis_e}"
                # Use standard logger for error
                logger.error("Gemini error analysis failed", exc_info=True)
                # End trace with failure
                agent_logger.end_operation(
                    analysis_trace, 
                    status="failed", 
                    error=str(analysis_e)
                )

            # === Step 2: Always Choose REGENERATE ===
            final_result = CorrectionResult(
                strategy=CorrectionStrategy.REGENERATE,
                success=True,
                explanation="Execution failed, attempting code regeneration.",
                analysis=analysis_text 
            )
            
        except Exception as e:
            # Catch unexpected errors before analysis even starts
            logger.error("Unexpected error during correction attempt setup", exc_info=True)
            final_result = CorrectionResult(
                strategy=CorrectionStrategy.ASK_USER, # Fallback if even setup fails
                success=False,
                explanation=f"Internal error setting up correction: {str(e)}",
                user_prompt="An internal error occurred before correction analysis could start. Cannot proceed automatically."
            )
            
        return final_result
    
    def _construct_analysis_prompt(
        self,
        original_request: str,
        failed_code: str,
        error_message: str,
        validation_failed: bool,
        schema_context: str
    ) -> str:
        error_source = "during validation check" if validation_failed else "during main code execution"
        
        return f"""
        You are an expert PySpark data engineer assisting an autonomous agent.
        Analyze the following situation where a Spark code execution failed.

        Original User Request:
        {original_request}

        Generated PySpark Code (that failed):
        ```python
        {failed_code}
        ```

        Database Schema Context (provided during code generation):
        {schema_context}

        Failure Occurred: {error_source}
        Error Message:
        ```
        {error_message}
        ```

        Provide a detailed analysis addressing these points:
        1.  **Root Cause:** What is the most likely reason for this specific error, considering the code, the schema, and whether it happened during execution or validation?
        2.  **Code Issue:** Identify the specific line(s) or logical part of the *generated code* that likely caused the error.
        3.  **Validation Check Issue (if applicable):** If the error occurred during validation, explain if the validation check itself might be flawed or if it correctly caught an issue in the generated code's output.
        4.  **Schema Relevance:** Does the error relate to misunderstanding or misuse of the provided database schema?
        5.  **Suggested Fix:** Briefly describe the conceptual fix needed (e.g., "correct column name", "handle null values", "change join condition", "fix validation assertion").
        6.  **Confidence:** Rate your confidence in this analysis (High/Medium/Low).

        Format your response clearly.
        """

# Create a singleton instance for global use
corrector = SelfCorrection() 