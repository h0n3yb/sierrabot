import unittest
from unittest.mock import patch, MagicMock, call
from src.agent_loop import AgentLoop, TransformationStatus, TransformationResult
from src.code_generator import GeneratedCode
from src.spark_executor import ExecutionResult
from src.self_correction import CorrectionResult, CorrectionStrategy
from unittest.mock import ANY

class TestAgentLoop(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the logger methods before creating AgentLoop
        self.mock_logger = MagicMock()
        self.mock_logger.info = MagicMock()
        self.mock_logger.error = MagicMock()
        self.mock_logger.warning = MagicMock()
        self.mock_logger.start_operation = MagicMock(return_value={"id": "test-trace"})
        self.mock_logger.end_operation = MagicMock()
        
        # Create a patcher for the logger
        self.logger_patcher = patch('src.agent_loop.agent_logger', self.mock_logger)
        self.logger_patcher.start()
        
        # Create the agent after patching the logger
        self.agent = AgentLoop(max_retries=3)
        
        # Common test data
        self.test_request = "show all customers"
        self.test_code = "SELECT * FROM customers"
        self.generated_code = GeneratedCode(
            code=self.test_code,
            description="Test transformation",
            tables_used=["customers"],
            estimated_complexity="low",
            validation_checks=["Result should not be empty"]
        )
        
        # Mock successful execution result
        self.success_execution = ExecutionResult(
            success=True,
            is_valid=True,
            data=MagicMock(to_dict=lambda: {"test": "data"}),
            execution_time=1.0,
            memory_used=100,
            rows_processed=10
        )
        
        # Mock failed execution result
        self.failed_execution = ExecutionResult(
            success=False,
            is_valid=False,
            error="Test error",
            execution_time=0.5,
            memory_used=50,
            rows_processed=0
        )

    def tearDown(self):
        """Clean up after each test method."""
        self.logger_patcher.stop()

    @patch('src.agent_loop.generator')
    @patch('src.agent_loop.executor')
    def test_successful_transformation(self, mock_executor, mock_generator):
        """Test a successful transformation flow"""
        # Setup mocks
        mock_generator.generate.return_value = self.generated_code
        mock_executor.execute.return_value = self.success_execution
        
        # Execute test
        result = self.agent.process_request(self.test_request)
        
        # Verify results
        self.assertEqual(result.status, TransformationStatus.SUCCESS)
        self.assertIsNotNone(result.data)
        self.assertEqual(result.data["result"], {"test": "data"})
        self.assertEqual(result.data["execution_time"], 1.0)
        self.assertEqual(result.data["memory_used"], 100)
        self.assertEqual(result.data["rows_processed"], 10)
        
        # Verify mock calls
        mock_generator.generate.assert_called_once_with(self.test_request)
        mock_executor.execute.assert_called_once_with(
            self.test_code,
            ["Result should not be empty"]
        )

    @patch('src.agent_loop.generator')
    def test_code_generation_failure(self, mock_generator):
        """Test handling of code generation failures"""
        # Setup mock to raise exception
        mock_generator.generate.side_effect = Exception("Code generation failed")
        
        # Execute test
        result = self.agent.process_request(self.test_request)
        
        # Verify results
        self.assertEqual(result.status, TransformationStatus.FAILED)
        self.assertEqual(result.message, "Failed to generate Spark code")
        self.assertEqual(result.error, "Code generation failed")

    @patch('src.agent_loop.generator')
    @patch('src.agent_loop.executor')
    @patch('src.agent_loop.corrector')
    def test_failed_execution_with_successful_retry(
        self, mock_corrector, mock_executor, mock_generator
    ):
        """Test handling of failed execution with successful retry"""
        # Setup mocks
        mock_generator.generate.return_value = self.generated_code
        mock_executor.execute.side_effect = [
            self.failed_execution,  # First attempt fails
            self.success_execution  # Retry succeeds
        ]
        
        # Mock successful correction
        mock_corrector.attempt_correction.return_value = CorrectionResult(
            success=True,
            strategy=CorrectionStrategy.MODIFY_CODE,
            explanation="Fixed query",
            modified_code=self.test_code
        )
        
        # Execute test
        result = self.agent.process_request(self.test_request)
        
        # Verify results
        self.assertEqual(result.status, TransformationStatus.SUCCESS)
        self.assertIsNotNone(result.data)
        
        # Verify correction was attempted
        mock_corrector.attempt_correction.assert_called_once()

    @patch('src.agent_loop.generator')
    @patch('src.agent_loop.executor')
    @patch('src.agent_loop.corrector')
    def test_max_retries_exceeded(
        self, mock_corrector, mock_executor, mock_generator
    ):
        """Test handling when max retries are exceeded"""
        # Setup mocks
        mock_generator.generate.return_value = self.generated_code
        mock_executor.execute.return_value = self.failed_execution
        
        # Mock failed correction
        mock_corrector.attempt_correction.return_value = CorrectionResult(
            success=False,
            strategy=CorrectionStrategy.RETRY_AS_IS,
            explanation="Could not correct",
            user_prompt=None
        )
        
        # Execute test with retry_count at max
        result = self.agent._handle_failed_execution(
            self.test_request,
            self.generated_code,
            self.failed_execution,
            retry_count=3  # Max retries is 3
        )
        
        # Verify results
        self.assertEqual(result.status, TransformationStatus.FAILED)
        self.assertEqual(result.message, "Max retries exceeded")
        self.assertEqual(result.retry_count, 3)

    @patch('src.agent_loop.generator')
    @patch('src.agent_loop.executor')
    @patch('src.agent_loop.corrector')
    def test_needs_user_clarification(
        self, mock_corrector, mock_executor, mock_generator
    ):
        """Test handling when user clarification is needed"""
        # Setup mocks
        mock_generator.generate.return_value = self.generated_code
        mock_executor.execute.return_value = self.failed_execution
        
        # Mock correction needing user input
        mock_corrector.attempt_correction.return_value = CorrectionResult(
            success=False,
            strategy=CorrectionStrategy.ASK_USER,
            explanation="Need more information",
            user_prompt="Please clarify the date range"
        )
        
        # Execute test
        result = self.agent.process_request(self.test_request)
        
        # Verify results
        self.assertEqual(result.status, TransformationStatus.FAILED)
        self.assertEqual(result.message, "Need user clarification")
        self.assertEqual(result.error, "Please clarify the date range")

    def test_invalid_request(self):
        """Test handling of invalid requests"""
        # Test with empty request
        result = self.agent.process_request("")
        
        self.assertEqual(result.status, TransformationStatus.FAILED)
        self.assertEqual(result.message, "Invalid transformation request")
        
        # Test with None
        result = self.agent.process_request(None)
        
        self.assertEqual(result.status, TransformationStatus.FAILED)
        self.assertEqual(result.message, "Invalid transformation request")

    @patch('src.agent_loop.generator')
    @patch('src.agent_loop.executor')
    def test_logging(self, mock_executor, mock_generator):
        """Test that operations are properly logged"""
        # Setup mocks
        mock_generator.generate.return_value = self.generated_code
        mock_executor.execute.return_value = self.success_execution
        
        # Execute test
        self.agent.process_request(self.test_request)
        
        # Verify initialization log
        self.mock_logger.info.assert_any_call(
            "Initializing AgentLoop",
            extra={"metadata": {"component": "agent_loop"}}
        )
        
        # Verify start operation logs
        expected_start_calls = [
            call("Starting operation", extra={
                "metadata": {
                    "operation": "process_transformation",
                    "component": "agent_loop",
                    "request_id": ANY
                }
            }),
            call("Starting operation", extra={
                "metadata": {
                    "operation": "code_generation",
                    "component": "agent_loop",
                    "request_id": ANY
                }
            }),
            call("Starting operation", extra={
                "metadata": {
                    "operation": "code_execution",
                    "component": "agent_loop",
                    "request_id": ANY
                }
            })
        ]
        self.mock_logger.info.assert_has_calls(expected_start_calls, any_order=False)
        
        # Verify end operation logs
        expected_end_calls = [
            call("Operation completed", extra={
                "metadata": {
                    "operation": "process_transformation",
                    "component": "agent_loop",
                    "request_id": ANY,
                    "success": True,
                    "is_valid": True,
                    "error": None,
                    "data": {
                        "transformation_result": ANY,
                        "validation_result": ANY
                    }
                }
            }),
            call("Operation completed", extra={
                "metadata": {
                    "operation": "code_generation",
                    "component": "agent_loop",
                    "request_id": ANY,
                    "success": True,
                    "is_valid": True,
                    "error": None,
                    "data": {
                        "generated_code": ANY
                    }
                }
            }),
            call("Operation completed", extra={
                "metadata": {
                    "operation": "code_execution",
                    "component": "agent_loop",
                    "request_id": ANY,
                    "success": True,
                    "error": None,
                    "data": {
                        "execution_result": ANY
                    }
                }
            })
        ]
        self.mock_logger.info.assert_has_calls(expected_end_calls, any_order=False)
        
        # Verify no errors were logged
        self.mock_logger.error.assert_not_called()
        self.mock_logger.warning.assert_not_called()

if __name__ == '__main__':
    unittest.main() 