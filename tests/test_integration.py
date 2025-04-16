import os
import sys

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

import unittest
from unittest.mock import patch, MagicMock, call
import json
import os
from src.agent_loop import AgentLoop, TransformationStatus
from src.code_generator import CodeGenerator, GeneratedCode
from src.spark_executor import ExecutionResult
from src.cli import cli
from src.web_app import app
from src.logger import AgentLogger
from click.testing import CliRunner
import pytest

class TestEndToEndFlow(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock environment variables
        os.environ["ANTHROPIC_API_KEY"] = "test_key"
        
        # Set up CLI runner
        self.cli_runner = CliRunner()
        
        # Set up Flask test client
        app.config['TESTING'] = True
        self.web_client = app.test_client()
        
        # Mock logger
        self.mock_logger = MagicMock()
        self.mock_logger.info = MagicMock()
        self.mock_logger.error = MagicMock()
        self.mock_logger.warning = MagicMock()
        self.mock_logger.start_operation = MagicMock(return_value={"id": "test-trace"})
        self.mock_logger.end_operation = MagicMock()
        
        # Create logger patcher
        self.logger_patcher = patch('src.agent_loop.agent_logger', self.mock_logger)
        self.logger_patcher.start()
        
        # Test data
        self.test_request = "show total sales by customer"
        self.test_code = """
        customers = read_table('customers')
        orders = read_table('orders')
        result_df = customers.join(orders, 'customer_id') \\
            .groupBy('customer_id', 'name') \\
            .agg(sum('amount').alias('total_sales'))
        """
        self.test_result = {
            "customer_id": [1, 2],
            "name": ["John", "Jane"],
            "total_sales": [1000, 2000]
        }

    def tearDown(self):
        """Clean up after each test method."""
        self.logger_patcher.stop()

    @patch('anthropic.Anthropic')
    @patch('src.code_generator.CodeGenerator.generate_transformation_code')
    @patch('src.spark_executor.SparkExecutor.execute')
    def test_cli_end_to_end_flow(self, mock_execute, mock_generate, mock_anthropic):
        """Test complete end-to-end flow using CLI interface"""
        # Setup mocks
        mock_generate.return_value = GeneratedCode(
            code=self.test_code,
            description="Calculate total sales per customer",
            tables_used=["customers", "orders"],
            estimated_complexity="medium",
            validation_checks=["Result should have customer_id, name, and total_sales columns"]
        )
        
        mock_execute.return_value = ExecutionResult(
            success=True,
            is_valid=True,
            data=MagicMock(toPandas=lambda: self.test_result),
            execution_time=1.0,
            memory_used=100,
            rows_processed=2
        )
        
        # Execute test through CLI
        result = self.cli_runner.invoke(cli, ['transform', self.test_request])
        
        # Verify CLI output
        assert result.exit_code == 0
        assert "Processing transformation request" in result.output
        assert "Transformation successful" in result.output
        assert "total_sales" in result.output
        
        # Verify logging flow
        self.mock_logger.info.assert_has_calls([
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
        ], any_order=False)

    @patch('anthropic.Anthropic')
    @patch('src.code_generator.CodeGenerator.generate_transformation_code')
    @patch('src.spark_executor.SparkExecutor.execute')
    def test_web_end_to_end_flow(self, mock_execute, mock_generate, mock_anthropic):
        """Test complete end-to-end flow using web interface"""
        # Setup mocks
        mock_generate.return_value = GeneratedCode(
            code=self.test_code,
            description="Calculate total sales per customer",
            tables_used=["customers", "orders"],
            estimated_complexity="medium",
            validation_checks=["Result should have customer_id, name, and total_sales columns"]
        )
        
        mock_execute.return_value = ExecutionResult(
            success=True,
            is_valid=True,
            data=MagicMock(toPandas=lambda: self.test_result),
            execution_time=1.0,
            memory_used=100,
            rows_processed=2
        )
        
        # Test initial page load
        response = self.web_client.get('/')
        assert response.status_code == 200
        assert b'SIERRA' in response.data
        
        # Test transformation request
        response = self.web_client.post('/api/transform',
                                      data=json.dumps({'request': self.test_request}),
                                      content_type='application/json')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'processing'
        
        # Test result retrieval
        response = self.web_client.get(f'/api/result/{data["request_id"]}')
        assert response.status_code == 200
        result_data = json.loads(response.data)
        assert result_data['status'] == 'success'
        assert 'total_sales' in str(result_data['data'])
        
        # Verify logging flow
        self.mock_logger.info.assert_has_calls([
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
        ], any_order=False)

class TestAgentTraceIntegration(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a real AgentLogger instance
        self.logger = AgentLogger()
        
        # Create a test agent with the real logger
        self.agent = AgentLoop(max_retries=3)
        
        # Test data
        self.test_request = "show total sales by customer"
        self.test_trace_id = "test-trace-123"

    def test_trace_correlation(self):
        """Test that all logs within an operation are properly correlated"""
        # Start a trace
        trace = self.logger.start_operation(
            "test_operation",
            metadata={"request": self.test_request},
            tags=["test", "integration"]
        )
        
        # Log some events within the trace
        self.logger.info(
            "Processing request",
            metadata={"step": "start"},
            session_id=trace["session_id"]
        )
        
        self.logger.info(
            "Generating code",
            metadata={"step": "code_gen"},
            session_id=trace["session_id"]
        )
        
        # End the trace
        self.logger.end_operation(
            trace,
            status="completed",
            result={"success": True}
        )
        
        # Verify log files
        with open(self.logger.log_file, 'r') as f:
            logs = f.readlines()
            # Check that all logs have the same trace ID
            session_id = trace["session_id"]
            for log in logs:
                log_data = json.loads(log)
                if "session_id" in log_data:
                    self.assertEqual(log_data["session_id"], session_id)

    def test_structured_logging(self):
        """Test that logs are properly structured for AgentTrace"""
        # Generate a log entry
        metadata = {
            "request": self.test_request,
            "complexity": "medium",
            "tables": ["customers", "orders"]
        }
        
        self.logger.info(
            "Starting transformation",
            metadata=metadata,
            tags=["transform", "start"],
            session_id=self.test_trace_id
        )
        
        # Read the last log entry
        with open(self.logger.log_file, 'r') as f:
            last_log = json.loads(f.readlines()[-1])
        
        # Verify log structure
        self.assertEqual(last_log["level"], "INFO")
        self.assertEqual(last_log["message"], "Starting transformation")
        self.assertEqual(last_log["metadata"]["request"], self.test_request)
        self.assertEqual(last_log["session_id"], self.test_trace_id)
        self.assertIn("timestamp", last_log)
        self.assertIn("transform", last_log["tags"])

    def test_error_logging(self):
        """Test error logging and trace correlation"""
        # Start a trace
        trace = self.logger.start_operation(
            "error_test",
            metadata={"request": self.test_request}
        )
        
        # Log an error
        error_msg = "Test error message"
        self.logger.error(
            error_msg,
            metadata={"error_type": "test_error"},
            session_id=trace["session_id"]
        )
        
        # End trace with error
        self.logger.end_operation(
            trace,
            status="failed",
            error=error_msg
        )
        
        # Verify error logs
        with open(self.logger.log_file, 'r') as f:
            logs = [json.loads(line) for line in f.readlines()]
            error_log = next(log for log in logs if log["level"] == "ERROR")
            
            self.assertEqual(error_log["message"], error_msg)
            self.assertEqual(error_log["metadata"]["error_type"], "test_error")
            self.assertEqual(error_log["session_id"], trace["session_id"])

    def test_performance_logging(self):
        """Test logging of performance metrics"""
        # Start a trace with performance tracking
        trace = self.logger.start_operation(
            "performance_test",
            metadata={
                "request": self.test_request,
                "expected_duration": "1s"
            }
        )
        
        # Log performance metrics
        self.logger.info(
            "Operation completed",
            metadata={
                "execution_time": 0.8,
                "memory_used": 100,
                "rows_processed": 1000
            },
            session_id=trace["session_id"]
        )
        
        # End trace with performance data
        self.logger.end_operation(
            trace,
            status="completed",
            result={
                "execution_time": 0.8,
                "memory_used": 100,
                "rows_processed": 1000
            }
        )
        
        # Verify performance metrics in logs
        with open(self.logger.log_file, 'r') as f:
            logs = [json.loads(line) for line in f.readlines()]
            perf_log = next(
                log for log in logs 
                if "execution_time" in log.get("metadata", {})
            )
            
            self.assertEqual(perf_log["metadata"]["execution_time"], 0.8)
            self.assertEqual(perf_log["metadata"]["memory_used"], 100)
            self.assertEqual(perf_log["metadata"]["rows_processed"], 1000)

if __name__ == '__main__':
    unittest.main() 