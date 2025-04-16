import unittest
from unittest.mock import patch, MagicMock, call
from src.schema_explorer import SchemaExplorer, ColumnInfo, TableStats

class TestSchemaExplorer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.connection_params = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'host': 'localhost',
            'port': '5432'
        }
        self.explorer = SchemaExplorer(self.connection_params)
        
        # Mock cursor results for different queries
        self.mock_tables_result = [
            {'table_name': 'customers'},
            {'table_name': 'orders'}
        ]
        
        self.mock_column_result = [
            {
                'column_name': 'id',
                'data_type': 'integer',
                'is_nullable': 'NO',
                'column_default': None,
                'character_maximum_length': None
            },
            {
                'column_name': 'name',
                'data_type': 'varchar',
                'is_nullable': 'YES',
                'column_default': None,
                'character_maximum_length': 100
            }
        ]
        
        self.mock_pk_result = [
            {'column_name': 'id'}
        ]
        
        self.mock_fk_result = [
            {
                'column_name': 'customer_id',
                'foreign_table_name': 'customers',
                'foreign_column_name': 'id'
            }
        ]
        
        self.mock_stats_result = [{
            'row_count': 1000,
            'avg_row_length': 100,
            'has_index': True,
            'last_analyzed': 1234567890
        }]
        
        self.mock_sample_values = {
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob']
        }

    @patch('psycopg2.connect')
    def test_get_all_tables(self, mock_connect):
        """Test retrieving all tables from the database."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = self.mock_tables_result
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Execute test
        tables = self.explorer.get_all_tables()
        
        # Verify results
        self.assertEqual(tables, {'customers', 'orders'})
        mock_cursor.execute.assert_called_once_with("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                    """)

    @patch('psycopg2.connect')
    def test_get_column_info(self, mock_connect):
        """Test retrieving column information for a table."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            self.mock_column_result,
            self.mock_pk_result,
            self.mock_fk_result
        ]
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Execute test
        columns = self.explorer.get_column_info('customers')
        
        # Verify results
        self.assertIsInstance(columns, dict)
        self.assertEqual(len(columns), 2)
        
        # Check id column
        id_column = columns['id']
        self.assertIsInstance(id_column, ColumnInfo)
        self.assertEqual(id_column.name, 'id')
        self.assertEqual(id_column.data_type, 'integer')
        self.assertFalse(id_column.is_nullable)
        self.assertTrue(id_column.is_primary_key)
        
        # Check name column
        name_column = columns['name']
        self.assertIsInstance(name_column, ColumnInfo)
        self.assertEqual(name_column.name, 'name')
        self.assertEqual(name_column.data_type, 'varchar')
        self.assertTrue(name_column.is_nullable)
        self.assertEqual(name_column.max_length, 100)
        self.assertFalse(name_column.is_primary_key)

    @patch('psycopg2.connect')
    def test_get_table_stats(self, mock_connect):
        """Test retrieving table statistics."""
        # Setup mock
        mock_cursor = MagicMock()
        # Mock results for table stats query
        mock_cursor.fetchone.return_value = self.mock_stats_result[0]
        # Mock results for column info queries and sample values
        mock_cursor.fetchall.side_effect = [
            self.mock_column_result,  # Basic column info
            self.mock_pk_result,      # Primary key info
            self.mock_fk_result,      # Foreign key info
            [{'id': 1}, {'id': 2}, {'id': 3}],  # Sample values for id
            [{'name': 'John'}, {'name': 'Jane'}, {'name': 'Bob'}]  # Sample values for name
        ]
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Execute test
        stats = self.explorer.get_table_stats('customers', sample_size=3)
        
        # Verify results
        self.assertIsInstance(stats, TableStats)
        self.assertEqual(stats.row_count, 1000)
        self.assertEqual(stats.avg_row_length, 100)
        self.assertTrue(stats.has_index)
        self.assertEqual(stats.last_analyzed, 1234567890)
        
        # Verify sample values were fetched
        self.assertIsNotNone(stats.sample_values)
        self.assertIn('id', stats.sample_values)
        self.assertIn('name', stats.sample_values)
        self.assertEqual(len(stats.sample_values['id']), 3)
        self.assertEqual(len(stats.sample_values['name']), 3)
        self.assertEqual(stats.sample_values['id'], [1, 2, 3])
        self.assertEqual(stats.sample_values['name'], ['John', 'Jane', 'Bob'])

    @patch('psycopg2.connect')
    def test_find_columns_by_type(self, mock_connect):
        """Test finding columns by data type."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {'table_name': 'customers', 'column_name': 'id'},
            {'table_name': 'orders', 'column_name': 'order_id'}
        ]
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Execute test
        columns = self.explorer.find_columns_by_type('integer')
        
        # Verify results
        self.assertEqual(columns, {
            'customers': ['id'],
            'orders': ['order_id']
        })
        mock_cursor.execute.assert_called_once_with("""
                        SELECT table_name, column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                        AND data_type = %s
                    """, ('integer',))

    @patch('psycopg2.connect')
    def test_get_table_relationships(self, mock_connect):
        """Test retrieving table relationships."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [{'from_column': 'customer_id', 'to_table': 'customers', 'to_column': 'id'}],
            [{'from_table': 'orders', 'from_column': 'customer_id', 'to_column': 'id'}]
        ]
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Execute test
        relationships = self.explorer.get_table_relationships('customers')
        
        # Verify results
        self.assertIn('outgoing', relationships)
        self.assertIn('incoming', relationships)
        self.assertEqual(len(relationships['outgoing']), 1)
        self.assertEqual(len(relationships['incoming']), 1)
        
        # Check outgoing relationship
        outgoing = relationships['outgoing'][0]
        self.assertEqual(outgoing['from_column'], 'customer_id')
        self.assertEqual(outgoing['to_table'], 'customers')
        self.assertEqual(outgoing['to_column'], 'id')
        
        # Check incoming relationship
        incoming = relationships['incoming'][0]
        self.assertEqual(incoming['from_table'], 'orders')
        self.assertEqual(incoming['from_column'], 'customer_id')
        self.assertEqual(incoming['to_column'], 'id')

    def test_format_schema_for_llm(self):
        """Test formatting schema for LLM consumption."""
        # Mock the required methods
        with patch.object(self.explorer, 'get_all_tables') as mock_get_tables, \
             patch.object(self.explorer, 'get_column_info') as mock_get_columns, \
             patch.object(self.explorer, 'get_table_relationships') as mock_get_relationships, \
             patch.object(self.explorer, 'get_table_stats') as mock_get_stats:
            
            # Setup mocks
            mock_get_tables.return_value = {'customers'}
            mock_get_columns.return_value = {
                'id': ColumnInfo('id', 'integer', False, None, None, True),
                'name': ColumnInfo('name', 'varchar', True, None, 100)
            }
            mock_get_relationships.return_value = {
                'outgoing': [],
                'incoming': [{'from_table': 'orders', 'from_column': 'customer_id', 'to_column': 'id'}]
            }
            mock_get_stats.return_value = TableStats(1000, 100, True, '1234567890')
            
            # Execute test
            schema_str = self.explorer.format_schema_for_llm(
                tables=['customers'],
                include_relationships=True,
                include_samples=False
            )
            
            # Verify results
            self.assertIn('customers (1,000 rows)', schema_str)
            self.assertIn('id (integer) (primary key, not null)', schema_str)
            self.assertIn('name (varchar(100))', schema_str)
            self.assertIn('Referenced by:', schema_str)
            self.assertIn('orders.customer_id → id', schema_str)

if __name__ == '__main__':
    unittest.main() 