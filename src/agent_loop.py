#!/usr/bin/env python3

from typing import Dict, Optional, Tuple, List, Any
from dataclasses import dataclass, field
from enum import Enum
from .code_generator import generator, GeneratedCode
from .spark_executor import executor, ExecutionResult
from .self_correction import corrector, CorrectionStrategy
from .logger import agent_logger
import time

class TransformationStatus(Enum):
    """Possible states of a transformation request"""
    PENDING = "pending"
    PROCESSING = "processing"
    VALIDATING = "validating"
    RETRYING = "retrying"
    FAILED = "failed"
    SUCCESS = "success"

@dataclass
class TransformationStepDetail:
    """Details of a single step in the transformation process"""
    name: str
    status: str  # e.g., 'completed', 'failed', 'skipped'
    metadata: Optional[Dict] = None
    error: Optional[str] = None

@dataclass
class TransformationResult:
    """Represents the result of a transformation operation"""
    status: TransformationStatus
    message: str
    data: Optional[Dict] = None
    error: Optional[str] = None
    retry_count: int = 0
    steps: List[TransformationStepDetail] = field(default_factory=list)

class AgentLoop:
    """
    Main orchestrator for the autonomous transformation process.
    Handles the flow from natural language to executed Spark code.
    """
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        agent_logger.info(
            "Initializing AgentLoop",
            metadata={"max_retries": max_retries},
            tags=["agent", "init"]
        )
    
    def cleanup(self):
        """Clean up resources before shutdown"""
        try:
            # Clean up the Spark executor
            if hasattr(executor, 'cleanup'):
                executor.cleanup()
            
            # Clean up any other resources
            agent_logger.info("AgentLoop cleanup completed")
        except Exception as e:
            agent_logger.error(f"Error during AgentLoop cleanup: {e}")
    
    def process_request(self, transformation_request: str, use_schema_tool: bool = False) -> TransformationResult:
        """
        Main entry point for processing a transformation request.
        Handles the full flow from analysis to execution and retries.
        """
        # --- Start Top-Level Trace ---
        trace = agent_logger.start_operation(
            "process_transformation_request",
            metadata={"transformation_request": transformation_request},
            tags=["agent", "orchestration"]
        )
        final_result: Optional[TransformationResult] = None # Initialize
        current_steps: List[TransformationStepDetail] = [] # Initialize steps here

        try:
            # Step 1 (Internal): Validate Request
            if not self._validate_request(transformation_request):
                error_msg = "Invalid transformation request (empty or whitespace only)"
                agent_logger.error(error_msg, tags=["agent", "validation", "error"])
                final_result = TransformationResult(
                    status=TransformationStatus.FAILED,
                    message=error_msg,
                    error=error_msg,
                    steps=current_steps
                )
                return final_result # Return early

            # Step 1 (Visible): Analyze Request
            step_analysis = TransformationStepDetail(name="Analyze Request", status="pending")
            current_steps.append(step_analysis)
            try:
                analysis_result = generator.analyze_request(
                    transformation_request,
                    use_schema_tool=use_schema_tool
                )
                if not analysis_result["tables"]:
                    error_msg = "No valid tables identified for the transformation"
                    step_analysis.status = "failed"
                    step_analysis.error = error_msg
                    final_result = TransformationResult(
                        status=TransformationStatus.FAILED,
                        message=error_msg,
                        error=error_msg,
                        steps=current_steps
                    )
                    return final_result # Return early
                step_analysis.status = "completed"
                step_analysis.metadata = {
                    "tables_identified": len(analysis_result["tables"]),
                    "steps_planned": len(analysis_result["transformation_steps"]),
                    "tables": analysis_result["tables"]
                }
            except Exception as e:
                error_msg = f"Failed to analyze transformation request: {e}"
                step_analysis.status = "failed"
                step_analysis.error = error_msg
                agent_logger.error(error_msg, exc_info=True, tags=["agent", "analysis", "error"])
                final_result = TransformationResult(
                    status=TransformationStatus.FAILED,
                    message=error_msg,
                    error=error_msg,
                    steps=current_steps
                )
                return final_result # Return early

            # Step 2: Generate Code (now with analysis context)
            step_codegen = TransformationStepDetail(name="Generate Code", status="pending")
            current_steps.append(step_codegen)
            try:
                generated_code = generator.generate_from_nl(
                    transformation_request,
                    analysis_context=analysis_result  # Pass the analysis context to code generation
                )
                step_codegen.status = "completed"
                step_codegen.metadata = {
                    "description": generated_code.description,
                    "tables_used": generated_code.tables_used,
                    "complexity": generated_code.estimated_complexity,
                    "code_preview": generated_code.code[:500] + ("..." if len(generated_code.code) > 500 else "")
                }
            except Exception as e:
                error_msg = f"Failed to generate Spark code: {e}"
                step_codegen.status = "failed"
                step_codegen.error = error_msg
                agent_logger.error(error_msg, exc_info=True, tags=["agent", "code_gen", "error"])
                final_result = TransformationResult(
                    status=TransformationStatus.FAILED,
                    message=error_msg,
                    error=error_msg,
                    steps=current_steps
                )
                return final_result # Return early

            # Step 3: Execute Code (Calls internal method that handles retries)
            step_execute = TransformationStepDetail(name="Execute Code", status="pending") # Add execute step placeholder
            current_steps.append(step_execute)
            final_result = self._execute_and_handle_retries(
                transformation_request,
                generated_code,
                current_steps, # Pass the list to be updated
                retry_count=0,
                analysis_context=analysis_result
            )
            return final_result # Return result from execution/retry logic

        except Exception as e:
            # Catch unexpected errors in the main process_request flow
            error_msg = f"Internal error processing request: {e}"
            for step in current_steps:
                if step.status == 'pending':
                    step.status = 'skipped'
            agent_logger.error(error_msg, exc_info=True, tags=["agent", "error"])
            final_result = TransformationResult(
                status=TransformationStatus.FAILED,
                message=error_msg,
                error=error_msg,
                steps=current_steps
            )
            return final_result # Return failure result

        finally:
            # --- End Top-Level Trace ---
            if final_result: # Ensure we have a result object to log
                agent_logger.end_operation(
                    trace,
                    status=final_result.status.value, # Use status enum value
                    result={ # Log the entire result object structure
                        "message": final_result.message,
                        "data": final_result.data, # This will include the final JSON data on success
                        "error": final_result.error,
                        "retry_count": final_result.retry_count,
                        "steps": [step.__dict__ for step in final_result.steps]
                    },
                    error=final_result.error if final_result.status != TransformationStatus.SUCCESS else None
                )
            else:
                 # Log if something went wrong and final_result wasn't assigned
                 agent_logger.end_operation(
                    trace,
                    status="unknown_error",
                    error="final_result object was not assigned during process_request",
                    result={"steps_attempted": [step.__dict__ for step in current_steps]}
                 )

    def _execute_and_handle_retries(
        self,
        transformation_request: str,
        initial_generated_code: GeneratedCode,
        current_steps: List[TransformationStepDetail],
        retry_count: int = 0,
        analysis_context: Dict[str, Any] = None
    ) -> TransformationResult:
        """
        Executes code, handles retries, and updates the steps list.
        NO LONGER TRACED at this top level.
        """
        # Find or add the execute step in the list
        step_execute = next((s for s in current_steps if s.name == "Execute Code"), None)
        if not step_execute:
            step_execute = TransformationStepDetail(name="Execute Code", status="pending")
            current_steps.append(step_execute)
        step_execute.status = "processing"
        step_execute.metadata = {"retry_attempt": retry_count + 1}
        
        try:
            current_code_to_execute = initial_generated_code.code
            current_validation_checks = initial_generated_code.validation_checks
            
            # Execute and unpack the result tuple
            start_time = time.time() # Track execution time
            is_valid, result_df, error_msg = executor.execute(
                current_code_to_execute,
                current_validation_checks
            )
            execution_time = time.time() - start_time
            
            # Determine success based on is_valid and lack of error message
            is_successful = is_valid and not error_msg
            
            # Update step metadata using unpacked values
            step_execute.metadata.update({
                "success": is_successful,
                "is_valid": is_valid, # is_valid directly from result
                "error": error_msg, # error_msg directly from result
                "execution_time": execution_time, 
                "rows_processed": result_df.count() if result_df is not None else 0
            })
            
            if is_successful:
                step_execute.status = "completed"
                # Step 5: Final Result Confirmation (implicitly done by returning success)
                step_confirm_result = TransformationStepDetail(name="Confirm Result", status="completed", metadata={"message": "Transformation successful."})
                current_steps.append(step_confirm_result)
                
                return TransformationResult(
                    status=TransformationStatus.SUCCESS,
                    message="Transformation completed successfully",
                    data={
                        # Convert Spark DataFrame to dict for JSON serialization
                        "result": result_df.limit(100).toPandas().to_dict('records') if result_df is not None else None,
                        "execution_time": execution_time,
                        "memory_used": "N/A", # Memory tracking removed from executor for now
                        "rows_processed": step_execute.metadata['rows_processed']
                    },
                    retry_count=retry_count,
                    steps=current_steps
                )
            else:
                # Execution failed or result invalid, proceed to correction/retry
                step_execute.status = "failed"
                step_execute.error = error_msg or "Execution succeeded but validation failed"
                agent_logger.warning(
                    f"Execution attempt {retry_count + 1} failed or invalid, attempting correction",
                    metadata=step_execute.metadata,
                    tags=["agent", "execution", "warning"]
                )
                # Prepare data for correction handler (using the new tuple format)
                failure_data = {
                    "is_valid": is_valid,
                    "error": error_msg,
                    "result_df": result_df # Pass the potentially partial/invalid DF
                }
                return self._handle_failed_execution(
                    transformation_request,
                    initial_generated_code, # Pass original code for context
                    failure_data, # Pass the new failure data dict
                    current_steps, # Pass steps list
                    retry_count,
                    analysis_context=analysis_context  # Pass analysis context to correction
                )
                
        except Exception as e:
            error_msg = f"Unexpected error during execution attempt {retry_count + 1}: {e}"
            step_execute.status = "failed"
            step_execute.error = error_msg
            agent_logger.error(error_msg, exc_info=True, tags=["agent", "execution", "error"])
            # No more retries on unexpected errors
            return TransformationResult(
                status=TransformationStatus.FAILED,
                message=error_msg,
                error=error_msg,
                retry_count=retry_count,
                steps=current_steps
            )

    def _handle_failed_execution(
        self,
        transformation_request: str,
        generated_code: GeneratedCode,
        failure_data: Dict[str, Any],
        current_steps: List[TransformationStepDetail],
        retry_count: int = 0,
        analysis_context: Dict[str, Any] = None
    ) -> TransformationResult:
        """
        Handles failed executions by attempting corrections and retries.
        NO LONGER TRACED at this top level.
        """
        # Add Correction Step
        step_correct = TransformationStepDetail(name=f"Attempt Correction (Retry {retry_count + 1})", status="pending")
        current_steps.append(step_correct)
        
        # Extract error message from failure data
        error_message = failure_data.get("error", "Unknown execution error")
        is_syntax_error = error_message.startswith("SYNTAX_ERROR:::")
        
        # If it's a syntax error, skip Gemini analysis and go straight to regenerate
        if is_syntax_error:
            agent_logger.warning("Syntax error detected in generated code. Skipping correction analysis, proceeding directly to regeneration.", tags=["agent", "retry", "syntax_error"])
            correction_result = CorrectionResult(
                strategy=CorrectionStrategy.REGENERATE,
                success=True, 
                explanation="Syntax error in generated code, attempting regeneration."
            )
            step_correct.status = "skipped"
            step_correct.metadata={"reason": "Syntax error detected", "strategy_chosen": "REGENERATE"}
            # No need for correction_trace here as we skipped the main correction logic
        else:
            # Proceed with Gemini-based correction analysis for other errors
            # Fetch the schema string that was used for generation
            schema_context = generator.format_schema_for_llm()
            
            # Prepare context for the corrector, including the schema
            context_for_corrector = {
                "tables_used": generated_code.tables_used,
                "complexity": generated_code.estimated_complexity,
                "retry_count": retry_count,
                "is_valid_result": failure_data.get("is_valid", False),
                "schema_context": schema_context, # Add schema context
                "analysis_context": analysis_context  # Pass analysis context to correction
            }
            
            if retry_count >= self.max_retries:
                final_error_msg = f"Max retries ({self.max_retries}) exceeded."
                step_correct.status = "failed"
                step_correct.error = final_error_msg
                agent_logger.error(final_error_msg, tags=["agent", "retry", "error"])
                # Ensure Execute step is marked failed
                exec_step = next((s for s in current_steps if s.name == "Execute Code"), None)
                if exec_step: exec_step.status = "failed"
                return TransformationResult(
                    status=TransformationStatus.FAILED,
                    message=final_error_msg,
                    error=failure_data.get("error", "Unknown execution error"), # Keep original error for context
                    retry_count=retry_count,
                    steps=current_steps
                )
            
            # Attempt correction using Gemini
            step_correct.status = "processing"
            
            try:
                correction_result = corrector.attempt_correction(
                    transformation_request,
                    generated_code.code, # Use original generated code for context
                    failure_data, # Pass the full failure data dict
                    context_for_corrector # Pass enriched context
                )
                
                step_correct.metadata = {
                    "strategy": correction_result.strategy.value if correction_result.success else None,
                    "explanation": correction_result.explanation,
                    "user_prompt": correction_result.user_prompt
                }
                
                if not correction_result.success:
                    step_correct.status = "failed"
                    step_correct.error = correction_result.explanation or correction_result.user_prompt
                    if correction_result.user_prompt:
                        agent_logger.warning("Need user clarification...", metadata=step_correct.metadata, tags=["agent", "correction", "warning"])
                        return TransformationResult(
                            status=TransformationStatus.FAILED, # Or a new status like PENDING_CLARIFICATION
                            message="Need user clarification",
                            error=correction_result.user_prompt,
                            retry_count=retry_count,
                            steps=current_steps
                        )
                    else:
                        agent_logger.error("Correction failed...", metadata=step_correct.metadata, tags=["agent", "correction", "error"])
                        return TransformationResult(
                            status=TransformationStatus.FAILED,
                            message="Correction failed",
                            error=correction_result.explanation,
                            retry_count=retry_count,
                            steps=current_steps
                        )
                
                step_correct.status = "completed"
                
            except Exception as e:
                # Catch unexpected errors during the correction process itself
                error_msg = f"Unexpected error during correction/retry phase: {e}"
                step_correct.status = "failed"
                step_correct.error = error_msg
                agent_logger.error(error_msg, exc_info=True, tags=["agent", "retry", "error"])
                return TransformationResult(
                    status=TransformationStatus.FAILED,
                    message=error_msg,
                    error=error_msg,
                    retry_count=retry_count,
                    steps=current_steps
                )
            # End of the 'else' block for non-syntax errors
        
        # --- Strategy Execution (Common for both syntax error and successful correction) --- 
        
        # Log chosen strategy regardless of how we got here
        agent_logger.info(f"Proceeding with strategy: {correction_result.strategy.value}", tags=["agent", "retry"])
        
        if correction_result.strategy == CorrectionStrategy.REGENERATE:
            # Generate new code from scratch, providing failure context if available
            regen_step = TransformationStepDetail(name=f"Regenerate Code (Retry {retry_count + 1})", status="processing")
            current_steps.append(regen_step)
            try:
                agent_logger.info("Attempting regeneration.", metadata={"is_syntax_error": is_syntax_error}, tags=["agent", "retry", "code_gen"])
                # Pass failure context if it was NOT just a syntax error (as analysis wasn't run)
                analysis_to_pass = None if is_syntax_error else (correction_result.analysis or "Analysis not available.")
                error_to_pass = "Syntax error in previous version." if is_syntax_error else failure_data.get("error", "Unknown error")
                
                modified_code = generator.regenerate_with_context(
                    original_request=transformation_request,
                    previous_code=generated_code.code,
                    error_message=error_to_pass,
                    analysis=analysis_to_pass or "Analysis skipped due to syntax error or failure."
                )
                regen_step.status="completed"
                regen_step.metadata = {"description": modified_code.description}
                agent_logger.info("Code regenerated successfully.", tags=["agent", "retry", "code_gen"])
            except Exception as e:
                # ... (handle regeneration failure as before) ...
                return TransformationResult(
                    # ... (result as before) ...
                )
            
            # Recursive call to execute the regenerated code
            return self._execute_and_handle_retries(
                transformation_request,
                modified_code, # Pass the regenerated code
                current_steps,
                retry_count + 1,
                analysis_context=analysis_context  # Pass analysis context to next retry
            )
        else: # Should only be ASK_USER if Gemini failed or explicitly chose it
            agent_logger.error(f"Correction resulted in unexpected strategy: {correction_result.strategy}. Failing.", tags=["agent", "retry", "error"])
            return TransformationResult(
                status=TransformationStatus.FAILED,
                message=f"Correction failed with unexpected strategy: {correction_result.strategy}",
                error=correction_result.explanation or "Unknown correction error",
                retry_count=retry_count,
                steps=current_steps
            )

    def _validate_request(self, request: str) -> bool:
        """Validates the transformation request"""
        return bool(request and request.strip())

# Create a singleton instance for global use
agent = AgentLoop() 