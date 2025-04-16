# Code Generation Module Documentation

## Overview

The code generation module is a core component of our autonomous data transformation agent. It uses Claude 3.5 to translate natural language transformation requests into executable Apache Spark code. The module employs a streamlined approach that focuses on:

- **Lightweight Schema Handling**: Efficient caching of essential table metadata
- **Output Validation**: Focus on validating transformation results rather than constraining inputs
- **Elite Code Quality**: Clean, maintainable, and efficient code generation
- **Secure Operations**: Enforced read-only database access

## Components

### TableMetadata Class

An immutable dataclass that stores essential table information:

```python
@dataclass(frozen=True)
class TableMetadata:
    name: str                    # Table name
    columns: Set[str]           # Set of column names
    primary_keys: Set[str]      # Set of primary key columns
    foreign_keys: Dict[str, tuple[str, str]]  # Column to (table, column) mapping
```

### GeneratedCode Class

Represents the output of code generation:

```python
@dataclass
class GeneratedCode:
    code: str                    # The generated Spark code
    description: str             # Description of what the code does
    tables_used: list[str]      # Tables accessed by the code
    estimated_complexity: str    # Complexity estimate (low/medium/high)
    validation_checks: list[str] # Validation rules for the output
```

### CodeGenerator Class

The main class responsible for code generation:

```python
class CodeGenerator:
    def generate_transformation_code(
        self,
        source_table: str,
        transformation_type: str,
        context: Optional[Dict] = None
    ) -> GeneratedCode:
        """Generates Spark code for a specific transformation type"""
```

Key features:
- Lightweight schema caching with 30-minute TTL
- On-demand foreign key resolution
- LRU caching for Claude API responses
- Comprehensive error handling
- Output-focused validation

## Usage Example

```python
from src.code_generator import generator

# Generate transformation code
generated = generator.generate_transformation_code(
    source_table="orders",
    transformation_type="revenue_by_category",
    context={"time_period": "last_month"}
)

# Access the generated code and metadata
print(f"Description: {generated.description}")
print(f"Tables used: {generated.tables_used}")
print(f"Complexity: {generated.estimated_complexity}")
print(f"Validation checks: {generated.validation_checks}")
```

## Schema Handling

The module uses a streamlined approach to schema handling:

1. **Basic Schema Caching**:
   - Caches table names, columns, and primary keys
   - 30-minute cache timeout
   - Single efficient SQL query for metadata

2. **On-Demand Foreign Keys**:
   - Foreign key information fetched only when needed
   - Separate query to minimize initial load
   - Results cached per table

3. **Validation Focus**:
   - Validates table existence before generation
   - Ensures required columns exist in output
   - Verifies non-empty results

## Generated Code Structure

The generated code follows a consistent pattern:

1. **Imports and Setup**:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd

spark = SparkSession.builder\\
    .appName("DataTransformation")\\
    .config("spark.jars", "postgresql-42.2.5.jar")\\
    .getOrCreate()
```

2. **Data Loading**:
```python
def read_table(table_name):
    """Read a table from PostgreSQL with proper error handling"""
    try:
        conn = get_postgres_connection()
        return spark.read.jdbc(
            url=conn["url"],
            table=table_name,
            properties=conn["properties"]
        )
    except Exception as e:
        logger.error(f"Failed to read table {table_name}", exc_info=True)
        raise RuntimeError(f"Could not read table {table_name}") from e
```

3. **Transformation Logic**:
```python
# Load source data
source_df = read_table('source_table')

# Apply transformations
result_df = source_df.transform(...)

# Validate results
assert set(result_df.columns) >= required_columns
assert result_df.count() > 0
```

## Error Handling

The module implements comprehensive error handling:

1. **Schema Errors**:
   - Table not found
   - Column validation failures
   - Foreign key resolution issues

2. **Generation Errors**:
   - Claude API failures
   - Response parsing issues
   - Invalid code generation

3. **Validation Errors**:
   - Missing columns
   - Empty results
   - Data type mismatches

## Environment Setup

Required environment variables:
- `ANTHROPIC_API_KEY`: Claude API key
- `POSTGRES_*`: Database connection details

## Best Practices

1. **Transformation Requests**:
   - Be specific about the transformation type
   - Provide relevant context
   - Specify any special requirements

2. **Performance**:
   - Use the LRU cache for identical requests
   - Monitor complexity estimates
   - Review generated code for efficiency

3. **Validation**:
   - Check validation results
   - Add custom validation if needed
   - Monitor execution outcomes

## Integration Example

```python
from src.code_generator import generator
from src.spark_executor import executor

# Generate and execute code
generated = generator.generate_transformation_code(
    source_table="orders",
    transformation_type="revenue_analysis",
    context={"group_by": "category"}
)

# Execute with validation
result = executor.execute(
    generated.code,
    generated.validation_checks
)
```

## Logging

The module uses structured logging:

```python
logger.info("Generating transformation code", extra={
    "source_table": source_table,
    "transformation_type": transformation_type,
    "context": context
})
```

All operations are logged with:
- Timestamp
- Operation type
- Input parameters
- Execution status
- Error details (if any)

## Database Schema

The code generator is aware of the following tables:

1. **customers**
   - customer_id (int, primary key)
   - name (varchar)
   - email (varchar)
   - date_joined (date)

2. **products**
   - product_id (int, primary key)
   - category (varchar: Electronics, Appliances, or Fitness)
   - name (varchar)
   - price (decimal)

3. **orders**
   - order_id (int, primary key)
   - customer_id (int, foreign key to customers)
   - order_date (timestamp)
   - total_amount (decimal)

4. **order_items**
   - order_id (int, foreign key to orders)
   - product_id (int, foreign key to products)
   - quantity (int)
   - unit_price (decimal)

5. **support_tickets**
   - ticket_id (int, primary key)
   - customer_id (int, foreign key to customers)
   - created_at (timestamp)
   - resolved_at (timestamp)
   - status (varchar: Open, In Progress, Resolved)
   - description (text)

## Generated Code Structure

The generated code follows a consistent structure:

1. Table loading using `read_table()`
2. Data transformations using PySpark operations
3. Result assignment to `result_df`
4. Conversion to pandas DataFrame
5. Error handling with try-except blocks

Example generated code:
```python
try:
    # Load tables
    products_df = read_table('products')
    orders_df = read_table('orders')
    order_items_df = read_table('order_items')
    
    # Calculate revenue per category
    result_df = products_df.join(
        order_items_df, 'product_id'
    ).join(
        orders_df, 'order_id'
    ).groupBy('category').agg(
        sum('total_amount').alias('total_revenue'),
        count('order_id').alias('order_count')
    ).orderBy(col('total_revenue').desc())
    
    # Convert to pandas
    result = result_df.toPandas()
except Exception as e:
    logger.error(f"Error during transformation: {str(e)}")
    raise
```

## Validation Checks

The code generator includes validation checks for the output data. Common checks include:

- Result should not be empty
- Required columns should be present
- No null values in critical columns
- Numeric values should be positive
- Date ranges should be valid

## Error Handling

The module implements comprehensive error handling:

1. **API Errors**: Handles Claude API failures gracefully
2. **Parsing Errors**: Validates JSON responses from Claude
3. **Code Generation Errors**: Catches and logs any issues during code generation
4. **Execution Errors**: Proper error propagation to the executor

## Testing

The module includes a comprehensive test suite in `tests/test_code_generator.py`:

```bash
# Run the tests
python -m unittest tests/test_code_generator.py
```

## Future Improvements

1. **Enhanced Schema Understanding**
   - Add support for more complex data types
   - Include table statistics
   - Support for custom validation rules

2. **Performance Optimization**
   - Implement more sophisticated caching
   - Add query complexity analysis
   - Optimize prompt engineering

3. **Code Quality**
   - Add more automated tests
   - Implement code style checking
   - Add performance benchmarks 