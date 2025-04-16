#!/usr/bin/env python3

import unittest
from unittest.mock import patch, MagicMock
import json
import os
from src.code_generator import CodeGenerator, GeneratedCode

class TestCodeGenerator(unittest.TestCase):
    def setUp(self):
        # Mock environment variables
        os.environ["ANTHROPIC_API_KEY"] = "test_key"
        self.generator = CodeGenerator()
    
    def test_initialization(self):
        """Test that CodeGenerator initializes correctly"""
        self.assertIsNotNone(self.generator)
        self.assertIsNotNone(self.generator.claude_client)
        self.assertIn("spark.jars", self.generator.spark_prelude)
        self.assertIn("customers", self.generator.schema_info)
    
    @patch('anthropic.Anthropic')
    def test_generate_simple_query(self, mock_anthropic):
        """Test code generation for a simple query"""
        # Mock Claude's response
        mock_response = {
            "code": "customers_df = read_table('customers')\nresult_df = customers_df.select('name', 'email')",
            "description": "Fetches customer names and emails",
            "tables_used": ["customers"],
            "estimated_complexity": "low",
            "validation_checks": ["Result should not be empty", "Required columns: name, email"]
        }
        
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text=json.dumps(mock_response))]
        mock_anthropic.return_value.messages.create.return_value = mock_message
        
        # Test code generation
        result = self.generator.generate("Get all customer names and emails")
        
        self.assertIsInstance(result, GeneratedCode)
        self.assertIn("customers_df = read_table('customers')", result.code)
        self.assertEqual(result.tables_used, ["customers"])
        self.assertEqual(result.estimated_complexity, "low")
        self.assertEqual(len(result.validation_checks), 2)
    
    @patch('anthropic.Anthropic')
    def test_generate_complex_query(self, mock_anthropic):
        """Test code generation for a complex query with joins"""
        # Mock Claude's response
        mock_response = {
            "code": """
# Load required tables
customers_df = read_table('customers')
orders_df = read_table('orders')
order_items_df = read_table('order_items')
products_df = read_table('products')

# Join tables and calculate metrics
result_df = customers_df.join(
    orders_df, 'customer_id'
).join(
    order_items_df, 'order_id'
).join(
    products_df, 'product_id'
).groupBy('customer_id', 'name').agg(
    sum('total_amount').alias('total_spent'),
    count('order_id').alias('order_count')
)""",
            "description": "Calculates total spent and order count per customer",
            "tables_used": ["customers", "orders", "order_items", "products"],
            "estimated_complexity": "high",
            "validation_checks": [
                "Result should not be empty",
                "Required columns: customer_id, name, total_spent, order_count",
                "No null values in total_spent",
                "All amounts should be positive"
            ]
        }
        
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text=json.dumps(mock_response))]
        mock_anthropic.return_value.messages.create.return_value = mock_message
        
        # Test code generation
        result = self.generator.generate(
            "Calculate total amount spent and number of orders for each customer"
        )
        
        self.assertIsInstance(result, GeneratedCode)
        self.assertIn("groupBy('customer_id'", result.code)
        self.assertEqual(len(result.tables_used), 4)
        self.assertEqual(result.estimated_complexity, "high")
        self.assertTrue(any("positive" in check for check in result.validation_checks))
    
    def test_prompt_construction(self):
        """Test that prompts are constructed correctly"""
        prompt = self.generator._construct_prompt("Test request")
        
        # Check that prompt contains key components
        self.assertIn("Test request", prompt)
        self.assertIn("Database Schema:", prompt)
        self.assertIn("Requirements:", prompt)
        self.assertIn("read_table()", prompt)
        self.assertIn("result_df", prompt)
    
    @patch('anthropic.Anthropic')
    def test_error_handling(self, mock_anthropic):
        """Test error handling in code generation"""
        # Mock Claude returning invalid JSON
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text="Invalid JSON")]
        mock_anthropic.return_value.messages.create.return_value = mock_message
        
        with self.assertRaises(ValueError):
            self.generator.generate("Test request")
    
    def test_cache_behavior(self):
        """Test that caching works for identical requests"""
        with patch('anthropic.Anthropic') as mock_anthropic:
            # First call
            mock_response = {
                "code": "result_df = read_table('customers')",
                "description": "Test",
                "tables_used": ["customers"],
                "estimated_complexity": "low",
                "validation_checks": ["Test check"]
            }
            mock_message = MagicMock()
            mock_message.content = [MagicMock(text=json.dumps(mock_response))]
            mock_anthropic.return_value.messages.create.return_value = mock_message
            
            # Make same request twice
            self.generator.generate("Test request")
            self.generator.generate("Test request")
            
            # Claude should only be called once due to caching
            self.assertEqual(
                mock_anthropic.return_value.messages.create.call_count,
                1
            )

if __name__ == '__main__':
    unittest.main() 