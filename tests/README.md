# Test Suite Documentation

This directory contains the test suites for the Sierra project. The tests are written using Python's `unittest` framework and use `pytest` as the test runner.

## Current Test Coverage

### Schema Explorer Tests (`test_schema_explorer.py`)

Tests the `SchemaExplorer` class which provides intelligent, progressive database schema exploration with clean, atomic operations.

#### Test Cases

1. `test_get_all_tables`
   - Tests retrieval of all tables from the database
   - Verifies correct SQL query execution
   - Ensures returned data structure is a set of table names

2. `test_get_column_info`
   - Tests detailed column information retrieval
   - Verifies handling of:
     - Primary key constraints
     - Data types
     - Nullability
     - Character length limits
     - Foreign key relationships

3. `test_get_table_stats`
   - Tests table statistics retrieval
   - Verifies:
     - Row count
     - Average row length
     - Index presence
     - Last analyzed timestamp
     - Sample value collection for columns

4. `test_find_columns_by_type`
   - Tests finding columns by specific data type across all tables
   - Verifies correct SQL query construction with type parameter
   - Ensures results are properly grouped by table

5. `test_get_table_relationships`
   - Tests relationship discovery between tables
   - Verifies both:
     - Incoming foreign key relationships
     - Outgoing foreign key relationships
   - Ensures relationship metadata is complete

6. `test_format_schema_for_llm`
   - Tests LLM-friendly schema formatting
   - Verifies formatting of:
     - Table statistics
     - Column definitions
     - Constraints
     - Relationships
   - Ensures output is properly structured for LLM consumption

### Agent Loop Tests (`test_agent_loop.py`)

Tests the autonomous agent loop that handles transformation requests, code generation, execution, and error handling.

#### Test Cases

1. `test_successful_transformation`
   - Tests complete successful transformation flow
   - Verifies code generation and execution
   - Checks result data structure and metadata

2. `test_code_generation_failure`
   - Tests handling of code generation errors
   - Verifies appropriate error messages
   - Ensures proper status updates

3. `test_failed_execution_with_successful_retry`
   - Tests retry mechanism after failed execution
   - Verifies self-correction behavior
   - Checks successful recovery path

4. `test_max_retries_exceeded`
   - Tests behavior when max retries are reached
   - Verifies appropriate error handling
   - Ensures proper status and message updates

5. `test_needs_user_clarification`
   - Tests scenarios requiring user input
   - Verifies user prompt generation
   - Checks clarification request handling

6. `test_invalid_request`
   - Tests handling of invalid/empty requests
   - Verifies input validation
   - Ensures appropriate error messages

7. `test_logging`
   - Tests comprehensive logging coverage
   - Verifies operation tracking
   - Checks metadata and tag handling

### Code Generator Tests (`test_code_generator.py`)

Tests the code generation component that translates natural language to Spark code using Claude.

#### Test Cases

1. `test_initialization`
   - Tests proper setup of CodeGenerator
   - Verifies environment variables
   - Checks schema information loading

2. `test_generate_simple_query`
   - Tests basic query generation
   - Verifies Claude API integration
   - Checks generated code structure

3. `test_generate_complex_query`
   - Tests complex query generation with joins
   - Verifies multi-table operations
   - Checks validation rules generation

4. `test_prompt_construction`
   - Tests LLM prompt formatting
   - Verifies schema inclusion
   - Checks requirement specifications

5. `test_error_handling`
   - Tests invalid response handling
   - Verifies error propagation
   - Checks exception handling

6. `test_cache_behavior`
   - Tests request caching mechanism
   - Verifies cache hits/misses
   - Checks performance optimization

### CLI Tests (`test_cli.py`)

Tests the command-line interface functionality.

#### Test Cases

1. `test_validate_transformation_request`
   - Tests input validation
   - Verifies handling of empty/invalid inputs
   - Checks validation rules

2. `test_transform_command`
   - Tests the main transformation command
   - Verifies command output
   - Checks error handling

3. `test_history_command`
   - Tests transformation history tracking
   - Verifies history display
   - Checks empty history handling

4. `test_version_command`
   - Tests version information display
   - Verifies version string format
   - Checks command output

### Web App Tests (`test_web_app.py`)

Tests the web-based dashboard interface.

#### Test Cases

1. `test_index_route`
   - Tests main dashboard page
   - Verifies page rendering
   - Checks basic content

2. `test_transform_api_empty_request`
   - Tests API input validation
   - Verifies error handling
   - Checks response format

3. `test_transform_api_valid_request`
   - Tests successful transformation requests
   - Verifies response structure
   - Checks status updates

4. `test_history_api`
   - Tests transformation history API
   - Verifies response format
   - Checks history data structure

## Mock Setup

The test suites use Python's `unittest.mock` to simulate external dependencies:

- Database connections (PostgreSQL)
- LLM API calls (Claude)
- File system operations
- Logging systems

## Test Data

Mock data includes:
```python
# Table definitions
mock_tables_result = [
    {'table_name': 'customers'},
    {'table_name': 'orders'}
]

# Column information
mock_column_result = [
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

# Statistics
mock_stats_result = [{
    'row_count': 1000,
    'avg_row_length': 100,
    'has_index': True,
    'last_analyzed': 1234567890
}]
```

## Running the Tests

To run all tests:
```bash
python -m pytest
```

To run tests with verbose output:
```bash
python -m pytest -v
```

To run a specific test file:
```bash
python -m pytest tests/test_schema_explorer.py
```

To run tests with coverage report:
```bash
python -m pytest --cov=src tests/
```

## Test Design Principles

1. **Isolation**: Each test is independent and uses fresh mock objects
2. **Completeness**: Tests cover both success and error paths
3. **Readability**: Tests are well-documented with clear setup and verification steps
4. **Maintainability**: Mock data is centralized in `setUp` for easy updates

## Future Test Coverage

Areas that could benefit from additional test coverage:

1. Error handling scenarios:
   - Database connection failures
   - Invalid table/column names
   - Permission issues
   - Timeout scenarios
   - API rate limiting
   - Network errors

2. Cache behavior:
   - Cache invalidation
   - TTL expiration
   - Cache hits vs. misses
   - Memory usage
   - Concurrent access

3. Edge cases:
   - Empty tables
   - Tables with no relationships
   - Complex data types
   - Special character handling
   - Unicode support
   - Large datasets

4. Performance tests:
   - Large schema handling
   - Multiple concurrent operations
   - Cache performance impact
   - Response time benchmarks
   - Resource utilization

5. Integration tests:
   - End-to-end workflows
   - Component interaction
   - State management
   - Error propagation

6. Security tests:
   - Input sanitization
   - SQL injection prevention
   - Access control
   - Rate limiting
   - Data validation 