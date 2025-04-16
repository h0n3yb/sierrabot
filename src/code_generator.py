#!/usr/bin/env python3
"""
Elite Spark Code Generator

This module implements a code generator for Apache Spark transformations. It
leverages Anthropic's Claude API to convert natural language data transformation
requests into efficient Spark code. It also employs schema caching and validation
to ensure consistent and robust code generation.

The overall transformation agent workflow is:
1. The user provides a transformation request in natural language.
2. The agent generates and executes a Spark transformation on PostgreSQL data.
3. The agent checks whether the transformation succeeded (e.g., verifying the
   output DataFrame).
4. If it fails, the agent autonomously reasons about the failure and retries or
   asks clarifying questions as needed.
5. Once a valid DataFrame is produced, the agent asks the user for confirmation.
"""

import logging
import json
import os
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Set, Any
from functools import lru_cache

import psycopg2
from psycopg2.extensions import connection
from pyspark.sql import DataFrame
from dotenv import load_dotenv

from anthropic import Anthropic
from .schema_explorer import SchemaExplorer
from .logger import agent_logger # Ensure logger is imported

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Tool Definition ---
SCHEMA_EXPLORATION_TOOL = {
    "name": "get_detailed_schema_info",
    "description": "Retrieves more detailed information about a specific table or column in the database schema. Use this tool if the initial schema overview is insufficient to determine precise filter values, join conditions, or data types. For example, use it to get distinct values for a categorical column mentioned in the request (like 'status' or 'type'), or to get detailed constraints for a column.",
    "input_schema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "The fully qualified name of the table (e.g., 'app_data.customers')."
            },
            "column_name": {
                "type": "string",
                "description": "Optional. The specific column name to get details for. If omitted, provides table-level details."
            },
            "information_needed": {
                "type": "array",
                "items": {"type": "string", "enum": ["distinct_values", "sample_data", "constraints", "data_type"]},
                "description": "Specify the type(s) of information required. 'distinct_values' is useful for categorical columns used in filters."
            }
        },
        "required": ["table_name", "information_needed"]
    }
}
# --- End Tool Definition ---

# Load environment variables
load_dotenv()


@dataclass(frozen=True)
class TableMetadata:
    """Immutable metadata for a database table."""
    name: str
    columns: Set[str] = field(default_factory=frozenset)
    primary_keys: Set[str] = field(default_factory=frozenset)
    foreign_keys: Dict[str, tuple[str, str]] = field(default_factory=dict)


@dataclass
class GeneratedCode:
    """Represents the generated Spark code and its associated metadata."""
    code: str
    description: str
    tables_used: List[str]
    estimated_complexity: str  # 'low', 'medium', 'high'
    validation_checks: List[str]


class CodeGenerator:
    """
    Elite-quality Spark code generator that converts natural language descriptions
    into robust Apache Spark transformations. This class manages schema caching,
    external API calls (to Anthropic's Claude), and output validation.
    """

    # Cache timeout in seconds (30 minutes)
    SCHEMA_CACHE_TIMEOUT = 1800

    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the code generator with required database configuration and
        set up the Claude client for code generation.

        Args:
            db_config: Dictionary of PostgreSQL connection parameters.
        """
        self.db_config = db_config
        self.schema_explorer = SchemaExplorer(db_config)
        self.claude_client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        logger.info("Initialized CodeGenerator with Anthropic Claude client.")

        # Standard Spark imports and setup; used as code prelude for every generated code.
        self.spark_imports = (
            "import pyspark\n" # Ensure base pyspark is available
            "import pandas as pd\n" # Allow generated code to use pandas
            "import os\n" # Allow generated code to use os
            # Explicit full paths will be required in generated code itself
        )
        self.spark_setup = (
            "# Initialize Spark session\n"
            "spark = SparkSession.builder\\\n"
            '    .appName("DataTransformation")\\\n'
            '    .config("spark.jars", "postgresql-42.2.5.jar")\\\n'
            "    .getOrCreate()\n\n"
            "# Define PostgreSQL connection parameters\n"
            "POSTGRES_PARAMS = {\n"
            "    \"url\": f\"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'localhost')}:"
            f"{os.environ.get('POSTGRES_PORT', '5432')}/{os.environ.get('POSTGRES_DB')}\",\n"
            "    \"properties\": {\n"
            "        \"user\": os.environ.get(\"POSTGRES_USER\"),\n"
            "        \"password\": os.environ.get(\"POSTGRES_PASSWORD\"),\n"
            "        \"driver\": \"org.postgresql.Driver\",\n"
            "        \"readonly\": \"true\"\n"
            "    }\n"
            "}\n"
        )
        self.spark_prelude = self.spark_imports + self.spark_setup

    def _get_db_connection(self) -> connection:
        """Create and return a read-only connection to PostgreSQL."""
        return psycopg2.connect(
            **self.db_config,
            options="-c default_transaction_read_only=on"
        )

    def _refresh_schema_cache(self) -> None:
        """
        Refresh the local schema cache if the cache has expired.
        Fetches table names, columns, and primary keys from PostgreSQL.
        """
        now = datetime.now()
        if (now - self._cache_timestamp).total_seconds() < self.SCHEMA_CACHE_TIMEOUT:
            return  # Cache is still valid

        try:
            with self._get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 
                            t.table_name,
                            array_agg(c.column_name ORDER BY c.ordinal_position) as columns,
                            array_agg(
                                CASE WHEN tc.constraint_type = 'PRIMARY KEY' 
                                THEN c.column_name 
                                ELSE NULL 
                                END
                            ) FILTER (WHERE tc.constraint_type = 'PRIMARY KEY') as pks
                        FROM information_schema.tables t
                        JOIN information_schema.columns c ON c.table_name = t.table_name
                        LEFT JOIN information_schema.table_constraints tc 
                            ON tc.table_name = t.table_name 
                            AND tc.constraint_type = 'PRIMARY KEY'
                        LEFT JOIN information_schema.constraint_column_usage ccu
                            ON ccu.constraint_name = tc.constraint_name 
                            AND ccu.column_name = c.column_name
                        WHERE t.table_schema = 'app_data'
                        GROUP BY t.table_name;
                        """
                    )
                    fetched_tables = cur.fetchall()
                    logger.info("Fetched %d table(s) from information_schema.", len(fetched_tables))
                    self._schema_cache.clear()
                    for table_name, columns, pks in fetched_tables:
                        self._schema_cache[table_name] = TableMetadata(
                            name=table_name,
                            columns=frozenset(columns),
                            primary_keys=frozenset(pk for pk in (pks or []) if pk)
                        )
                    self._cache_timestamp = now
                    logger.info("Schema cache refreshed with %d table(s).", len(self._schema_cache))
        except Exception as e:
            logger.error("Failed to refresh schema cache", exc_info=True)
            raise RuntimeError("Schema cache refresh failed") from e

    def _get_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """
        Get metadata for a specific table, refreshing cache if needed.
        Returns None if the table is not found.
        """
        self._refresh_schema_cache()
        return self._schema_cache.get(table_name)

    def _fetch_foreign_keys(self, table_name: str) -> Dict[str, tuple[str, str]]:
        """
        Lazily fetch and return foreign key metadata for a given table.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A dictionary mapping column names to a tuple of (foreign_table, foreign_column).
        """
        try:
            with self._get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            kcu.column_name,
                            ccu.table_name AS foreign_table_name,
                            ccu.column_name AS foreign_column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                            ON tc.constraint_name = kcu.constraint_name
                        JOIN information_schema.constraint_column_usage ccu
                            ON ccu.constraint_name = tc.constraint_name
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                            AND tc.table_name = %s;
                        """, (table_name,)
                    )
                    return {
                        col: (ftable, fcol)
                        for col, ftable, fcol in cur.fetchall()
                    }
        except Exception as e:
            logger.error("Failed to fetch foreign keys for table '%s'.", table_name, exc_info=True)
            return {}

    def _construct_prompt(self, request: str) -> tuple[str, str]:
        """
        Construct the system and user prompts for standard code generation.
        Focuses on core requirements and asks for RAW CODE output.
        Returns a tuple: (system_prompt, user_prompt)
        """
        # Fetch dynamically discovered schema information
        schema_info = self.schema_explorer.format_schema_for_llm(
            include_relationships=True,
            include_samples=False,
            include_stats=False
        )
        
        system_prompt = f"""
You are an expert Apache Spark developer generating PySpark code.

<core_requirements>
1.  **Load Data:** Use the `read_table(qualified_table_name)` function provided implicitly.
2.  **Handle Ambiguity (CRITICAL):**
    - When joining **any** DataFrames (base tables *or* intermediate results, **including the results of `select` or `withColumn` operations performed immediately before the join**) that could have overlapping column names (like 'id', 'name', 'customer_id', etc.):
    - **You MUST alias both input DataFrames immediately before the join operation** (e.g., `df1_prepared.alias('a').join(df2_prepared.alias('b'), ...)`). "Prepared" means after any necessary `select` or `withColumn` relevant to the join.
    - **You MUST use these aliases consistently** to reference columns in the join condition AND in ALL subsequent operations (`select`, `filter`, `groupBy`, etc.) on the joined DataFrame.
    - See examples below for correct aliasing.
3.  **Final Output:** Assign the final transformed Spark DataFrame to a variable named exactly `result`.
4.  **Output:** Respond ONLY with the raw PySpark code block continuation after the prefilled prelude. Do NOT include explanations, comments outside the code, or markdown formatting.
</core_requirements>

<general_guidelines>
- Write efficient and readable PySpark code.
- Use full explicit paths for ALL PySpark components (e.g., `pyspark.sql.functions.col`, `pyspark.sql.window.Window`, `pyspark.sql.types.StringType`). Do NOT rely on imports within the generated code snippet.
- Include comments within the code explaining complex logic.
- If catching errors, assign the exception to `execution_error` or re-raise.
</general_guidelines>

<example name="Base Table Join">
*** Example of Correct Base Table Join Handling ***
Request: Join customers and orders on customer_id and show order details for 'John Doe'.
Schema Snippets:
app_data.customers: id (PK), name, email
app_data.orders: order_id (PK), customer_id (FK->customers.id), order_date, amount

Correct Code Snippet:
```python
# Load data using read_table and alias immediately
customers_df = read_table('app_data.customers').alias('c')
orders_df = read_table('app_data.orders').alias('o')
# Perform join using aliases
joined_df = customers_df.join(
    orders_df,
    pyspark.sql.functions.col('c.id') == pyspark.sql.functions.col('o.customer_id'), # Use aliases in join condition
    'inner'
)
# Filter and select using aliases
result = joined_df.filter(pyspark.sql.functions.col('c.name') == 'John Doe') \
                  .select(
                      pyspark.sql.functions.col('c.name'), 
                      pyspark.sql.functions.col('o.order_id'), 
                      pyspark.sql.functions.col('o.order_date'), 
                      pyspark.sql.functions.col('o.amount')
                  )
```
*** End Base Table Example ***
</example>

<example name="Intermediate Join">
*** Example of Correct Intermediate Join Handling ***
Scenario: Joining intermediate results `agg_spend` (cols: cust_id, name, total_spend) and `pref_cat` (cols: cust_id, name, category).

Correct Code Snippet:
```python
# Assume agg_spend and pref_cat are previously defined DataFrames

# MANDATORY: Alias intermediate DFs immediately before joining
aliased_spend = agg_spend.alias('s')
aliased_pref = pref_cat.alias('p')

# Join using aliases
joined_intermediate = aliased_spend.join(
    aliased_pref,
    pyspark.sql.functions.col('s.cust_id') == pyspark.sql.functions.col('p.cust_id'), # Use aliases
    'inner'
)

# Select using aliases to resolve ambiguity from the join
final_output = joined_intermediate.select(
    pyspark.sql.functions.col('s.cust_id'),      # Or p.cust_id, usually doesn't matter for join key
    pyspark.sql.functions.col('s.name').alias('customer_name'), # Select one 'name' and alias it clearly
    pyspark.sql.functions.col('s.total_spend'),
    pyspark.sql.functions.col('p.category')
)
# Assign to result
result = final_output
```
*** End Intermediate Example ***
</example>

<example name="Window Function Usage">
*** Example of Correct Window Function Path Usage ***
```python
# Define the window specification using the full path
window_spec = pyspark.sql.window.Window.partitionBy('dept_id').orderBy(pyspark.sql.functions.col('salary').desc())

# Apply the window function using the full path for rank
ranked_df = employees_df.withColumn(
    'rank',
    pyspark.sql.functions.rank().over(window_spec)
)
result = ranked_df
```
*** End Window Function Example ***
</example>

<example name="Complex Intermediate Join Reference">
*** Example of Correct Referencing After Multiple Joins/Aliases ***
Scenario:
1. df_A (id, val_a) aliased 'a'
2. df_B (id, val_b) aliased 'b'
3. df_C (id, val_c) aliased 'c'
4. Join 'a' and 'b' -> intermediate_df (selecting a.id as common_id, val_a, val_b) aliased 'iab'
5. Join 'iab' and 'c'

Correct Code Snippet:
```python
# Assume df_A, df_B, df_C exist

# First join
intermediate_df = df_A.alias('a').join(
    df_B.alias('b'),
    pyspark.sql.functions.col('a.id') == pyspark.sql.functions.col('b.id'),
    'inner'
).select(
    pyspark.sql.functions.col('a.id').alias('common_id'), # Rename potentially ambiguous id
    pyspark.sql.functions.col('a.val_a'),
    pyspark.sql.functions.col('b.val_b')
).alias('iab') # Alias the result of the first join + select

# Second join
final_df = intermediate_df.join(
    df_C.alias('c'),
    # CORRECT: Reference 'common_id' directly via 'iab' alias.
    # DO NOT use nested aliases like 'iab.a.id'.
    pyspark.sql.functions.col('iab.common_id') == pyspark.sql.functions.col('c.id'),
    'inner'
)

# Final Selection using the aliases from the *last* join
result = final_df.select(
    pyspark.sql.functions.col('iab.common_id'),
    pyspark.sql.functions.col('iab.val_a'),
    pyspark.sql.functions.col('c.val_c') # 'val_b' is also available via 'iab.val_b'
)
```
*** End Complex Intermediate Join Example ***
</example>

<example name="Avoiding Nested Aggregations">
*** Example of Correctly Handling Aggregates of Aggregates ***
Scenario: Calculate the total count of items across all orders for each customer.

Incorrect Code (Nested Aggregation - Causes Error):
```python
# This will likely cause an AnalysisException
result = orders_df.alias('o').join(
    order_items_df.alias('oi'),
    pyspark.sql.functions.col('o.id') == pyspark.sql.functions.col('oi.order_id')
).groupBy('o.customer_id').agg(
    # INCORRECT: Nesting SUM(COUNT(...)) or similar is not allowed directly
    pyspark.sql.functions.sum(
        pyspark.sql.functions.count('oi.id') # Inner aggregate inside outer aggregate
    ).alias('total_items_across_orders')
)
```

Correct Code (Multi-Step Aggregation):
```python
# Step 1: Calculate the count per order first
items_per_order = order_items_df.groupBy('order_id').agg(
    pyspark.sql.functions.count('id').alias('item_count')
).alias('ipo')

# Step 2: Join with orders and then sum the per-order counts for each customer
result = orders_df.alias('o').join(
    items_per_order,
    pyspark.sql.functions.col('o.id') == pyspark.sql.functions.col('ipo.order_id'),
    'inner'
).groupBy('o.customer_id').agg(
    # CORRECT: Sum the result of the previous aggregation
    pyspark.sql.functions.sum('ipo.item_count').alias('total_items_across_orders')
)
```
*** End Avoiding Nested Aggregations Example ***
</example>
"""

        user_prompt = f"""
Generate the PySpark code for the following request, using the provided schema. Adhere strictly to the requirements and examples in the system prompt.

<transformation_request>
{request}
</transformation_request>

<schema_info>
{schema_info}
</schema_info>
"""
        return system_prompt.strip(), user_prompt.strip()

    def _construct_regenerate_prompt(
        self,
        original_request: str,
        previous_code: str,
        error_message: str,
        analysis: str
    ) -> tuple[str, str]:
        """
        Construct the system and user prompts for code REGENERATION.
        Focuses on correcting the error based on analysis, asks for RAW CODE.
        Returns a tuple: (system_prompt, user_prompt)
        """
        # Fetch schema info again for the regeneration prompt
        schema_info = self.schema_explorer.format_schema_for_llm(
            include_relationships=True,
            include_samples=False,
            include_stats=False
        )
        
        system_prompt = f"""
You are an expert Apache Spark developer REGENERATING PySpark code based on a previous failed attempt.

<core_requirements>
1.  **Address Failure:** Focus on correcting the root cause identified in the `<failure_analysis>` from the user prompt, paying close attention to resolving any ambiguity errors or missing path errors.
2.  **Load Data:** Use the `read_table(qualified_table_name)` function.
3.  **Handle Ambiguity (CRITICAL):** Review the `<failure_analysis>`. Ensure you **alias BOTH base tables AND intermediate DataFrames before joining them** if there's any chance of overlapping column names. Use these aliases consistently in join conditions and ALL subsequent operations (`select`, `filter`, `groupBy`).
4.  **Final Output:** Assign the final transformed Spark DataFrame to `result`.
5.  **Output:** Respond ONLY with the corrected raw PySpark code block continuation. Do NOT include explanations or markdown formatting.
</core_requirements>

<general_guidelines>
- Write efficient and readable PySpark code.
- Use full explicit paths for ALL PySpark components (e.g., `pyspark.sql.functions.col`, `pyspark.sql.window.Window`, `pyspark.sql.types.StringType`). Do NOT rely on imports within the generated code snippet.
- Include comments within the code clarifying the fix or complex logic, especially around alias usage for joins or corrected paths.
</general_guidelines>

<example name="Window Function Usage - Reminder">
*** Reminder: Use Full Paths for Window Functions ***
```python
# Correct: Use full path for Window
window_spec = pyspark.sql.window.Window.partitionBy('dept_id').orderBy(pyspark.sql.functions.col('salary').desc())

# Apply using full path for rank
ranked_df = employees_df.withColumn(
    'rank',
    pyspark.sql.functions.rank().over(window_spec)
)
result = ranked_df
```
*** End Window Function Reminder ***
</example>

<example name="Complex Intermediate Join Reference - Reminder">
*** Reminder: Referencing After Multiple Joins/Aliases ***
Scenario:
1. Join 'a' and 'b' -> intermediate_df (selecting a.id as common_id, val_a, val_b) aliased 'iab'
2. Join 'iab' and 'c'

Correct Code Snippet for Second Join:
```python
# Second join
final_df = intermediate_df.alias('iab').join( # Ensure intermediate is aliased if not already
    df_C.alias('c'),
    # CORRECT: Reference 'common_id' directly via 'iab' alias.
    # DO NOT use nested aliases like 'iab.a.id'.
    pyspark.sql.functions.col('iab.common_id') == pyspark.sql.functions.col('c.id'),
    'inner'
)
result = final_df # Assign final result
```
*** End Complex Intermediate Join Reminder ***
</example>

<example name="Avoiding Nested Aggregations - Reminder">
*** Reminder: Avoid Nested Aggregations ***
Incorrect:
```python
# Error: Using count() inside sum() in the same agg()
.agg(pyspark.sql.functions.sum(pyspark.sql.functions.count('item_id')).alias('total'))
```
Correct:
```python
# Step 1: Count per group
step1_df = df.groupBy('group_col', 'sub_group_col').agg(pyspark.sql.functions.count('item_id').alias('count_per_sub'))
# Step 2: Sum the counts
result = step1_df.groupBy('group_col').agg(pyspark.sql.functions.sum('count_per_sub').alias('total'))
```
*** End Nested Aggregations Reminder ***
</example>
"""
        
        user_prompt = f"""
Your previous attempt failed. Generate a NEW version addressing the failure context, paying special attention to resolving ambiguity through aliasing.

<original_request>
{original_request}
</original_request>

<previous_code>
{previous_code}
</previous_code>

<error_message>
{error_message}
</error_message>

<failure_analysis>
{analysis}
</failure_analysis>

<schema_info>
{schema_info}
</schema_info>
"""
        return system_prompt.strip(), user_prompt.strip()

    def _generate_code_with_prompt(self, system_prompt: str, user_prompt: str, cache_key: Optional[str] = None) -> str:
        """
        Internal method to call Claude API using prefilling with the Spark prelude.
        Returns the FULL code string (prelude + generated part).
        """
        trace = agent_logger.start_operation(
            "llm_code_generation_call_prefilled", # Renamed operation
            metadata={
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "cache_key_provided": bool(cache_key),
                "prelude_length": len(self.spark_prelude)
            },
            tags=["llm", "code_gen", "claude", "prefilled_output"] # Updated tags
        )

        # Cache key logic remains similar, but uses the prompts directly
        full_cache_key = f"prefilled_sys:{system_prompt}|user:{user_prompt}" if cache_key else None

        # --- Cache Check ---
        if full_cache_key and hasattr(self, '_llm_cache_code') and full_cache_key in self._llm_cache_code:
             agent_logger.info(f"Prefilled Code Cache hit for key: {cache_key}", tags=["llm", "cache", "hit", "code_cache"])
             cached_full_code = self._llm_cache_code[full_cache_key]
             agent_logger.end_operation(
                 trace, 
                 status="completed_cached", 
                 result={"full_code_length": len(cached_full_code)}
             )
             return cached_full_code

        if not hasattr(self, '_llm_cache_code'):
             self._llm_cache_code = {}

        # --- API Call with Prefilling ---
        claude_continuation = ""
        raw_text_content = ""
        try:
            agent_logger.info("Sending prompt to Claude for prefilled code...", tags=["llm", "api_call", "prefilled"])
            logger.debug("System Prompt for Claude (Prefilled Code): %s", system_prompt)
            logger.debug("User Prompt for Claude (Prefilled Code): %s", user_prompt)
            logger.debug("Prefilling Assistant message with prelude (first 100 chars): %r...", self.spark_prelude[:100])

            messages = [
                {"role": "user", "content": user_prompt},
                {"role": "assistant", "content": self.spark_prelude.rstrip()}
            ]

            # Conditionally build arguments for the API call
            api_args = {
                "model": "claude-3-5-sonnet-latest",
                "max_tokens": 4096,
                "temperature": 0,
                "system": system_prompt,
                "messages": messages
            }

            response = self.claude_client.messages.create(
                **api_args # Unpack the arguments
            )

            # Extract Claude's *continuation*
            raw_text_content = self._extract_text_content(response.content)
            logger.info("Received raw continuation text from Claude: %r", raw_text_content)

            # Strip markdown fences ONLY if they surround the continuation part
            if raw_text_content.startswith("```python") and raw_text_content.endswith("```"):
                claude_continuation = raw_text_content[len("```python\n"):-len("\n```")]
            elif raw_text_content.startswith("```") and raw_text_content.endswith("```"):
                 claude_continuation = raw_text_content[len("```\n"):-len("\n```")]
            else:
                claude_continuation = raw_text_content # Assume it's just the code continuation

            claude_continuation = claude_continuation.strip() # Clean the continuation

            # Construct the FULL code
            full_code = self.spark_prelude + "\n" + claude_continuation

            # Cache the FULL code
            if full_cache_key:
                self._llm_cache_code[full_cache_key] = full_code

            agent_logger.end_operation(
                trace, 
                status="completed", 
                result={
                    "raw_continuation_response": raw_text_content, # Log raw continuation
                    "extracted_continuation": claude_continuation, # Log extracted continuation
                    "full_code_generated": full_code # Log the final combined code
                }
            )
            return full_code
            
        except Exception as e:
            agent_logger.error("Claude API call or prefilled code extraction failed", exc_info=True, tags=["llm", "error", "prefilled_output"])
            if full_cache_key: self._llm_cache_code.pop(full_cache_key, None)
            agent_logger.end_operation(
                trace, 
                status="failed", 
                error=str(e), 
                result={
                    "raw_continuation_response_before_error": raw_text_content,
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt,
                    "prelude_used": self.spark_prelude # Log prelude for context
                }
            )
            raise RuntimeError("Prefilled code generation failed") from e

    def generate_from_nl(self, request: str, analysis_context: Optional[Dict[str, Any]] = None) -> GeneratedCode:
        """
        Generate Spark transformation code directly from a natural language request using prefilling.
        Uses analysis context if provided. Orchestrates only the code generation call.
        """
        full_code = "" # Initialize full_code
        try:
            # Step 1: Generate the full code using prefilling
            if analysis_context and analysis_context.get("tables") and analysis_context.get("table_metadata"):
                # Format metadata and steps for the user prompt (logic remains the same)
                # --- Start Restored Formatting Logic ---
                metadata_context = []
                for table in analysis_context["tables"]:
                    metadata = analysis_context["table_metadata"][table]
                    table_info = [f"\nTable: {table}"]
                    table_info.append("\nColumns:")
                    for col_name, col_info in metadata["columns"].items():
                        constraints = []
                        if col_info["is_primary_key"]: constraints.append("PK")
                        if not col_info["is_nullable"]: constraints.append("NOT NULL")
                        if col_info["foreign_key_reference"]:
                            ref = col_info["foreign_key_reference"]
                            constraints.append(f"FK to {ref[0]}({ref[1]})")
                        constraint_str = f" [{', '.join(constraints)}]" if constraints else ""
                        table_info.append(f"  - {col_name}: {col_info['data_type']}{constraint_str}")
                    if metadata.get("relationships") and metadata["relationships"].get("incoming"):
                        refs = [f"{rel['from_table']}.{rel['from_column']}" for rel in metadata["relationships"]["incoming"]]
                        table_info.append(f"\nReferenced by: {', '.join(refs)}")
                    if metadata.get("relationships") and metadata["relationships"].get("outgoing"):
                        refs = [f"{rel['to_table']}.{rel['to_column']} (via {rel['from_column']})" for rel in metadata["relationships"]["outgoing"]]
                        table_info.append(f"\nReferences: {', '.join(refs)}")
                    metadata_context.append("\n".join(table_info))
                transformation_steps = analysis_context.get("transformation_steps", [])
                metadata_context_str = "\n".join(metadata_context)
                steps_context_str = "\n".join(f"Step {i+1}: {step}" for i, step in enumerate(transformation_steps))
                # --- End Restored Formatting Logic ---
                full_code = self._cached_generate_prefilled( # Use new helper
                    request,
                    metadata_context=metadata_context_str,
                    transformation_steps=steps_context_str
                )
            else:
                full_code = self._cached_generate_prefilled(request) # Use new helper

            if not full_code or not isinstance(full_code, str):
                 raise ValueError("Prefilled code generation failed to return a valid string.")

            # Step 2: Assemble the GeneratedCode object (NO METADATA CALL)
            # Define basic validation checks that operate on the 'df' passed to the validation scope
            validation_checks = [
                "assert df is not None",
                "assert isinstance(df, DataFrame)"
            ]

            # Create GeneratedCode with placeholder metadata
            final_generated_code = GeneratedCode(
                code=full_code, # Already includes prelude
                description="Code generated via prefilling.", # Placeholder
                tables_used=[], # Placeholder - could potentially parse `read_table` calls if needed later
                estimated_complexity="unknown", # Placeholder
                validation_checks=validation_checks
            )
            logger.info(f"Successfully generated prefilled code object: {final_generated_code!r}")
            return final_generated_code

        except Exception as e:
            logger.error("Failed to generate code from natural language using prefilling", exc_info=True)
            error_detail = f"Prefilled code generation failed. Code generated before error (if any):\n---\n{full_code}\n---"
            raise RuntimeError(error_detail) from e

    # Renamed helper for clarity
    def _cached_generate_prefilled(
        self,
        request: str,
        metadata_context: Optional[str] = None,
        transformation_steps: Optional[str] = None
    ) -> str: # Returns the full code string
        """
        Helper to generate Spark code using prefilling, handling context and caching.
        Constructs system/user prompts and calls _generate_code_with_prompt.
        """
        system_prompt, base_user_prompt = self._construct_prompt(request)
        user_prompt = base_user_prompt

        if metadata_context or transformation_steps:
            context_sections = []
            if metadata_context:
                context_sections.append(f"<additional_table_metadata>\n{metadata_context}\n</additional_table_metadata>")
            if transformation_steps:
                 context_sections.append(f"<proposed_transformation_steps>\n{transformation_steps}\n</proposed_transformation_steps>")
            user_prompt += "\n\n" + "\n".join(context_sections)

        cache_key_base = request # Simple cache key based on request for now

        # Call the main prefilling generation method
        return self._generate_code_with_prompt(system_prompt, user_prompt, cache_key=cache_key_base)


    def format_schema_for_llm(self) -> str:
        """
        Format the current schema cache into a string for inclusion in prompts.
        NO LONGER TRACED.
        """
        try:
            # Call explorer method directly
            result = self.schema_explorer.format_schema_for_llm(
                include_relationships=True,
                include_samples=False
            )
            return result
        except Exception as e:
            # Log standard error, but no longer end trace
            logger.error("Schema formatting failed", exc_info=True)
            raise

    def analyze_request(self, request: str, use_schema_tool: bool = True) -> Dict[str, Any]:
        """
        Analyze a transformation request in two phases:
        1. Identify affected tables
        2. Generate detailed transformation steps using table metadata
        
        Args:
            request: The natural language transformation request

        Returns:
            Dictionary containing:
            - tables: List of affected tables
            - transformation_steps: List of steps to perform the transformation
            - table_metadata: Dict of table information including columns and relationships
        """
        trace = agent_logger.start_operation(
            "analyze_transformation_request",
            metadata={"request": request},
            tags=["llm", "analysis", "claude"]
        )
        
        # Check if trace_obj is valid (has an id attribute) before accessing .id
        parent_trace_id = getattr(trace, 'id', None) 
        if parent_trace_id is None:
            logger.warning("Initial trace object for analyze_request is invalid or missing 'id'. Nested traces won't have parent_trace_id.")

        result = {
            "tables": [],
            "transformation_steps": [],
            "table_metadata": {}
        }
        raw_table_response_text = "" # Initialize
        raw_steps_response_text = "" # Initialize
        claude_call_count = 0 # Track calls within this analysis
        tool_results_content = [] # To store tool results if used
        messages = [] # To build the conversation for step generation
        
        try:
            available_tables = list(self.schema_explorer.get_all_tables())

            # Phase 1: Identify affected tables
            table_system_prompt = "You are an expert at analyzing data transformation requests and accurately identifying the involved tables from a provided list. Respond ONLY with a JSON object containing a 'tables' key with an array of table names."
            table_user_prompt = f"""
Analyze this data transformation request and identify which tables from the available tables would be involved.
Focus ONLY on identifying the necessary tables, not the transformation steps yet.

<request>
{request}
</request>

<available_tables>
{available_tables}
</available_tables>

Respond with a JSON object containing ONLY an array of table names that would be involved in this transformation.
Example response: {{"tables": ["customers", "orders"]}}
"""
            # First LLM call to identify tables
            claude_call_count += 1
            table_call_trace = agent_logger.start_operation(
                f"claude_analysis_call_{claude_call_count}_table_id",
                metadata={
                    "parent_trace_id": parent_trace_id,
                    "prompt_length": len(table_user_prompt),
                    "system_prompt_length": len(table_system_prompt)
                },
                tags=["llm", "api_call", "claude", "analysis", "table_identification"]
            )
            table_response = self.claude_client.messages.create(
                model="claude-3-5-sonnet-latest",
                max_tokens=4096,
                temperature=0,
                system=table_system_prompt,
                messages=[{"role": "user", "content": table_user_prompt}]
            )
            agent_logger.end_operation(table_call_trace, status="completed", result={"response_stop_reason": table_response.stop_reason})

            # Extract and validate the tables using existing parser
            raw_table_response_text = self._extract_text_content(table_response.content)
            try:
                parsed_response = self._parse_claude_response(table_response.content)
                identified_tables = parsed_response.get("tables", [])
                if not identified_tables:
                    raise ValueError("No tables identified in the response")
            except Exception as e:
                logger.error("Failed to parse table identification response", exc_info=True)
                raise ValueError(f"Invalid response from table identification: {e}")

            valid_tables = [table for table in identified_tables 
                          if table in self.schema_explorer.get_all_tables()]
            
            for table in set(identified_tables) - set(valid_tables):
                logger.warning("Identified table '%s' does not exist in the current schema.", table)
            
            if not valid_tables:
                raise ValueError("No valid tables identified for the transformation")
            
            result["tables"] = valid_tables
            
            # Collect metadata for identified tables
            for table in valid_tables:
                table_metadata = {}
                
                # Get column information
                try:
                    columns = self.schema_explorer.get_column_info(table)
                    table_metadata["columns"] = {
                        col_name: {
                            "data_type": info.data_type,
                            "is_nullable": info.is_nullable,
                            "is_primary_key": info.is_primary_key,
                            "foreign_key_reference": info.foreign_key_reference
                        }
                        for col_name, info in columns.items()
                    }
                except Exception as e:
                    logger.warning(f"Could not fetch column info for table {table}: {e}")
                    table_metadata["columns"] = {}

                # Get relationships
                try:
                    relationships = self.schema_explorer.get_table_relationships(table)
                    table_metadata["relationships"] = relationships
                except Exception as e:
                    logger.warning(f"Could not fetch relationships for table {table}: {e}")
                    table_metadata["relationships"] = {"incoming": [], "outgoing": []}

                result["table_metadata"][table] = table_metadata

            # Phase 2: Generate transformation steps
            # Format the metadata for the prompt (re-added logic)
            metadata_context = []
            for table in valid_tables:
                metadata = result["table_metadata"][table]
                table_info = [f"\nTable: {table}"]
                
                # Add column information
                table_info.append("\nColumns:")
                for col_name, col_info in metadata["columns"].items():
                    constraints = []
                    if col_info["is_primary_key"]:
                        constraints.append("PK")
                    if not col_info["is_nullable"]:
                        constraints.append("NOT NULL")
                    if col_info["foreign_key_reference"]:
                        ref = col_info["foreign_key_reference"]
                        constraints.append(f"FK to {ref[0]}({ref[1]})")
                    
                    constraint_str = f" [{', '.join(constraints)}]" if constraints else ""
                    table_info.append(f"  - {col_name}: {col_info['data_type']}{constraint_str}")
                
                # Add relationship information
                if metadata["relationships"]["incoming"]:
                    refs = [f"{rel['from_table']}.{rel['from_column']}" 
                           for rel in metadata["relationships"]["incoming"]]
                    table_info.append(f"\nReferenced by: {', '.join(refs)}")
                
                if metadata["relationships"]["outgoing"]:
                    refs = [f"{rel['to_table']}.{rel['to_column']} (via {rel['from_column']})" 
                           for rel in metadata["relationships"]["outgoing"]]
                    table_info.append(f"\nReferences: {', '.join(refs)}")
                
                metadata_context.append("\n".join(table_info))
                
            # Join the collected metadata context into a single string
            metadata_context_str = "\n".join(metadata_context)

            steps_system_prompt = "You are an expert at analyzing data transformations and creating detailed step-by-step plans that reference specific database structures and relationships provided in the user message. You have access to a tool to get more schema details if needed. Respond ONLY with a JSON object containing a 'transformation_steps' key with an array of strings detailing the steps, OR use the provided tool if more schema information is required." # Updated system prompt
            steps_user_prompt = f"""
Analyze this data transformation request and break it down into clear, logical steps.
Use the detailed table information provided below. If this information is insufficient to determine precise filter values (e.g., for categorical columns like 'status' or 'type'), use the 'get_detailed_schema_info' tool to request more details like distinct values BEFORE generating the steps.

<request>
{request}
</request>

<available_table_information>
{metadata_context_str}
</available_table_information>

Respond with a JSON object containing an array of detailed steps explaining how to perform this transformation, OR use the tool to get more info.

Example JSON response:
{{{{
    "transformation_steps": [
        "Load customer_orders table, selecting columns customer_id (PK) and order_date",
        "Join with customers table using customer_id as the foreign key relationship",
        "Filter where order_date is within the last 30 days",
        "Group by customer_name to calculate order statistics"
    ]
}}}}
"""
            # Add the initial user prompt for step generation to messages
            messages.append({"role": "user", "content": steps_user_prompt})

            # Make the API call, providing the tool
            claude_call_count += 1
            step_call_trace = agent_logger.start_operation(
                f"claude_analysis_call_{claude_call_count}_step_gen_initial",
                metadata={
                    "parent_trace_id": parent_trace_id,
                    "prompt_length": len(steps_user_prompt),
                    "system_prompt_length": len(steps_system_prompt),
                    "tools_provided": 1
                },
                tags=["llm", "api_call", "claude", "analysis", "step_generation", "tool_use_enabled"]
            )
            try:
                # Conditionally build arguments for the API call
                api_args = {
                    "model": "claude-3-5-sonnet-latest",
                    "max_tokens": 4096,
                    "temperature": 0,
                    "system": steps_system_prompt,
                    "messages": messages
                }
                if use_schema_tool:
                    api_args["tools"] = [SCHEMA_EXPLORATION_TOOL]

                steps_response = self.claude_client.messages.create(
                    **api_args # Unpack the arguments
                )
                agent_logger.end_operation(step_call_trace, status="completed", result={"response_stop_reason": steps_response.stop_reason})
            except Exception as api_e:
                agent_logger.end_operation(step_call_trace, status="failed", error=str(api_e))
                raise # Re-raise the exception to be caught by the outer try block

            raw_steps_response_text = self._extract_text_content(steps_response.content) # Store raw response

            # Check if Claude wants to use the tool
            response_to_parse = None # Initialize
            while steps_response.stop_reason == "tool_use":
                agent_logger.info("Claude requested tool use for schema details.", tags=["llm", "tool_use", "schema"])
                # Add Claude's response (containing the tool use request) to messages
                messages.append({"role": "assistant", "content": steps_response.content})

                # Process the tool use request
                tool_use_block = next((block for block in steps_response.content if block.type == "tool_use"), None)
                if not tool_use_block:
                     raise ValueError("Stop reason was 'tool_use', but no tool_use block found.")

                tool_name = tool_use_block.name
                tool_input = tool_use_block.input
                tool_use_id = tool_use_block.id

                if tool_name == SCHEMA_EXPLORATION_TOOL["name"]:
                    # Execute the tool locally
                    tool_success, tool_result_data = self._execute_schema_tool(tool_input)

                    # Add the tool result to messages
                    tool_result_message_content = {
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": json.dumps(tool_result_data), # Send result/error data as JSON string
                    }
                    # Add is_error flag if the tool execution failed
                    if not tool_success:
                        tool_result_message_content["is_error"] = True

                    messages.append({
                        "role": "user",
                        "content": [tool_result_message_content]
                    })

                    # Make the next API call with the tool result included
                    agent_logger.info("Sending tool results back to Claude.", tags=["llm", "tool_result", "schema"])
                    claude_call_count += 1
                    follow_up_call_trace = agent_logger.start_operation(
                        f"claude_analysis_call_{claude_call_count}_step_gen_follow_up",
                        metadata={
                            "parent_trace_id": parent_trace_id, 
                            "messages_length": len(messages), 
                            "system_prompt_length": len(steps_system_prompt), 
                            "tools_provided": 1,
                            "tool_result_id": tool_use_id
                        },
                        tags=["llm", "api_call", "claude", "analysis", "step_generation", "tool_result_provided"]
                    )
                    try:
                        # This response might trigger another tool use or be the final one
                        # Conditionally build arguments for the API call
                        follow_up_api_args = {
                            "model": "claude-3-5-sonnet-latest",
                            "max_tokens": 4096,
                            "temperature": 0,
                            "system": steps_system_prompt,
                            "messages": messages
                        }
                        if use_schema_tool:
                            follow_up_api_args["tools"] = [SCHEMA_EXPLORATION_TOOL]

                        steps_response = self.claude_client.messages.create(
                            **follow_up_api_args # Unpack the arguments
                        )
                        agent_logger.end_operation(follow_up_call_trace, status="completed", result={"response_stop_reason": steps_response.stop_reason})
                    except Exception as api_e:
                        agent_logger.end_operation(follow_up_call_trace, status="failed", error=str(api_e))
                        raise # Re-raise to be caught by outer try
                    # Loop continues if stop_reason is still 'tool_use'

                else:
                    # Handle unexpected tool name if necessary
                    error_msg = f"Claude requested unknown tool: {tool_name}"
                    logger.error(error_msg)
                    agent_logger.update_operation(trace, status="failed", error=error_msg)
                    # Break the loop on unknown tool
                    response_to_parse = steps_response.content # Attempt to parse whatever we got before unknown tool
                    break # Exit while loop
            else:
                # Loop finished because stop_reason was not 'tool_use'
                response_to_parse = steps_response.content
                raw_steps_response_text = self._extract_text_content(response_to_parse)
                agent_logger.info("Tool use loop completed. Final response received.", tags=["llm", "tool_use", "final_response"])

            # Ensure response_to_parse is set before attempting to parse
            if response_to_parse is None:
                # This case might happen if the loop broke unexpectedly
                raise ValueError("No final response content available to parse after tool use loop.")

            # Extract the transformation steps using existing parser from the final response
            try:
                parsed_steps = self._parse_claude_response(response_to_parse)
                result["transformation_steps"] = parsed_steps.get("transformation_steps", [])
            except Exception as e:
                logger.error(f"Failed to parse final transformation steps response after tool use loop. Content: {response_to_parse!r}", exc_info=True)
                # Store the raw text if parsing fails, for debugging
                result["transformation_steps"] = [f"ERROR: Failed to parse final steps response. Raw content received: {raw_steps_response_text}"]
                # Update the main trace to indicate partial failure
                agent_logger.update_operation(trace, error=f"Failed to parse final steps: {e}")

            # Log success with final results (even if steps parsing failed, we got this far)
            agent_logger.end_operation(
                trace,
                status="completed",
                result={
                    "tables_identified": len(valid_tables),
                    "steps_generated": len(result["transformation_steps"]),
                    "tables": valid_tables,
                    "metadata_collected": bool(result["table_metadata"]),
                    "transformation_steps": result["transformation_steps"],
                    "table_metadata_summary": {
                        table: {
                            "columns_count": len(metadata["columns"]),
                            "relationships": {
                                "incoming": len(metadata["relationships"]["incoming"]),
                                "outgoing": len(metadata["relationships"]["outgoing"])
                            }
                        }
                        for table, metadata in result["table_metadata"].items()
                    }
                }
            )
            return result

        except Exception as e:
            error_msg = f"Failed to analyze request: {str(e)}"
            logger.error(error_msg, exc_info=True)
            agent_logger.end_operation(
                trace,
                status="failed",
                error=error_msg,
                # Add raw responses if available at point of failure
                result={
                    "raw_table_response_before_error": raw_table_response_text,
                    "raw_steps_response_before_error": raw_steps_response_text
                }
            )
            return result # Return partially filled result on error

    def _extract_text_content(self, content: Any) -> str:
        """
        Extract the text content from the Claude API response.

        Args:
            content: The content field from the Claude response.

        Returns:
            A string containing the extracted text.
        """
        if content and isinstance(content, list):
            # Find first block of type 'text'
            for block in content:
                if hasattr(block, "type") and block.type == "text" and hasattr(block, "text"):
                    return block.text
            return "No valid text block found in response content."
        return f"Unexpected content format: {type(content)}"

    def _parse_claude_response(self, response_content: Any) -> Dict[str, Any]:
        """
        Parse the response content from Claude into a structured Python dictionary.
        Handles responses that may include planning sections or other non-JSON content.

        Args:
            response_content: The raw response content from Claude.

        Returns:
            A dictionary with keys 'code', 'description', 'tables_used', 'complexity', and 'validation_checks'.

        Raises:
            ValueError: If the response content does not meet the expected format or cannot be parsed.
        """
        try:
            if not response_content or not isinstance(response_content, list):
                raise ValueError("Unexpected response format from Claude: Content is not a list.")

            # Locate the text block containing the JSON
            text_block = next(
                (block for block in response_content if hasattr(block, "type") and block.type == "text"), None
            )

            if not text_block or not hasattr(text_block, "text"):
                raise ValueError("Unexpected response format from Claude: No text block found.")

            json_string = text_block.text.strip()
            
            # Find the actual start of the JSON after the planning block
            planning_end_tag = '</approach_planning>'
            planning_end_index = json_string.rfind(planning_end_tag)
            search_start_index = 0
            if planning_end_index != -1:
                search_start_index = planning_end_index + len(planning_end_tag)
            
            # Find the first '{' or '[' after the planning block
            start_brace = json_string.find('{', search_start_index)
            start_bracket = json_string.find('[', search_start_index)
            
            start_index = -1
            if start_brace != -1 and start_bracket != -1:
                start_index = min(start_brace, start_bracket)
            elif start_brace != -1:
                start_index = start_brace
            elif start_bracket != -1:
                start_index = start_bracket
                
            if start_index == -1:
                # Fallback: If no bracket/brace found after planning, try the old rfind method
                # as a last resort, though this is less likely to be correct.
                logger.warning("Could not find JSON start after planning tag, falling back to rfind.")
                start_index = json_string.rfind('{')
                if start_index == -1:
                    start_index = json_string.rfind('[')
                    if start_index == -1:
                        raise ValueError(f"Invalid content: No JSON object or array found after planning tag or via rfind. Text: {json_string!r}")

            # Find the matching closing bracket/brace using stack (starts from the identified start_index)
            stack = []
            in_string = False
            escape_next = False
            end_index = -1
            
            for i in range(start_index, len(json_string)):
                char = json_string[i]
                    
                if escape_next:
                    escape_next = False
                    continue
                    
                if char == '\\':
                    escape_next = True
                    continue
                    
                if char == '"' and not escape_next:
                    in_string = not in_string
                    continue
                    
                if not in_string:
                    if char in '{[':
                        stack.append(char)
                    elif char in '}]':
                        if not stack:
                            break
                        if (char == '}' and stack[-1] == '{') or (char == ']' and stack[-1] == '['):
                            stack.pop()
                            if not stack:  # Found the matching close
                                end_index = i + 1
                                break
                        else:
                            break  # Mismatched brackets
            
            if end_index == -1:
                raise ValueError(f"Invalid JSON content: Could not find matching closing bracket/brace. Text: {json_string!r}")
                 
            # Extract the JSON block
            json_block = json_string[start_index:end_index]
            logger.debug("Attempting to parse extracted JSON block (raw): %r", json_block)
            
            # Strip leading/trailing whitespace before parsing
            json_block_stripped = json_block.strip()
            logger.debug("Attempting to parse extracted JSON block (stripped): %r", json_block_stripped)
            
            try:
                parsed = json.loads(json_block_stripped) # Parse the stripped version
                # If we got a list (from table identification), wrap it in a dict
                if isinstance(parsed, list):
                    return {"tables": parsed}
                return parsed
            except json.JSONDecodeError as e:
                # Log the problematic block for easier debugging
                logger.error("JSON parsing failed. Block attempting to parse (stripped): %r", json_block_stripped)
                logger.error("JSON parsing failed on extracted block. Raw text: %r, Extracted block: %r", json_string, json_block, exc_info=True)
                raise ValueError(f"Invalid JSON content within extracted block: {e}") from e

        except Exception as e:
            logger.error("Error parsing Claude response: %r", response_content, exc_info=True)
            raise ValueError(f"Error parsing Claude response: {e}") from e

    def _execute_schema_tool(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the 'get_detailed_schema_info' tool based on input from Claude.
        Fetches requested details using SchemaExplorer.

        Args:
            tool_input: Dictionary containing table_name, information_needed, and optional column_name.

        Returns:
            Tuple containing a success boolean and a dictionary with the requested
            schema information or error messages.
        """
        tool_trace = agent_logger.start_operation(
            "execute_schema_tool",
            metadata={"tool_input": tool_input},
            tags=["tool_execution", "schema", "internal"]
        )
        results = {}
        try:
            table_name = tool_input["table_name"]
            info_needed = tool_input["information_needed"]
            column_name = tool_input.get("column_name") # Optional

            agent_logger.info(f"Executing schema tool for table '{table_name}' (column: {column_name or 'N/A'}), requesting: {info_needed}", tags=["tool_execution"])

            for info_type in info_needed:
                try:
                    if info_type == "distinct_values":
                        if column_name:
                            # Call the new method in SchemaExplorer
                            distinct_values = self.schema_explorer.get_distinct_column_values(table_name, column_name)
                            results["distinct_values"] = {column_name: distinct_values}
                            logger.info(f"Fetched {len(distinct_values)} distinct values for {table_name}.{column_name}")
                        else:
                            results["distinct_values"] = "Error: column_name is required to get distinct values."
                            logger.warning("Distinct values requested without column name for table {table_name}")
                    elif info_type == "sample_data":
                        # Use get_table_stats to retrieve sample data
                        stats = self.schema_explorer.get_table_stats(table_name, sample_size=5)
                        if stats and stats.sample_values:
                            if column_name:
                                # If column specified, return samples for that column
                                results["sample_data"] = {column_name: stats.sample_values.get(column_name, [])}
                            else:
                                # If no column specified, return all samples
                                results["sample_data"] = stats.sample_values
                            logger.info(f"Fetched sample data for {table_name} (column: {column_name or 'all'})")
                        else:
                            results["sample_data"] = f"Could not fetch sample data for {table_name}."
                            logger.warning(f"Sample data requested but not retrieved for {table_name}")
                    elif info_type in ["constraints", "data_type"]:
                        if column_name:
                            col_info = self.schema_explorer.get_column_info(table_name)
                            if column_name in col_info:
                                column_details = col_info[column_name]
                                if info_type == "data_type":
                                    results["data_type"] = {column_name: column_details.data_type}
                                elif info_type == "constraints":
                                    constraints = []
                                    if column_details.is_primary_key: constraints.append("PRIMARY KEY")
                                    if not column_details.is_nullable: constraints.append("NOT NULL")
                                    if column_details.foreign_key_reference: constraints.append(f"FOREIGN KEY to {column_details.foreign_key_reference[0]}({column_details.foreign_key_reference[1]})")
                                    results["constraints"] = {column_name: constraints or ["No specific constraints found"]}
                            else:
                                results[info_type] = f"Error: Column '{column_name}' not found in table '{table_name}'."
                        else:
                            results[info_type] = f"Error: column_name is required to get {info_type}."
                    else:
                        results[info_type] = f"Error: Unknown information type requested: {info_type}"
                except Exception as e:
                    error_msg = f"Error fetching '{info_type}' for {table_name}{'.' + column_name if column_name else ''}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    results[info_type] = error_msg

            agent_logger.end_operation(tool_trace, status="completed", result=results)
            # Return True for success, along with results
            return True, results

        except KeyError as e:
            error_msg = f"Missing required key in tool input: {e}"
            logger.error(error_msg, exc_info=False)
            results["error"] = error_msg
            agent_logger.end_operation(tool_trace, status="failed", error=error_msg, result=results)
            # Return False for failure, along with error info
            return False, results
        except Exception as e:
            error_msg = f"Unexpected error executing schema tool: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results["error"] = error_msg
            agent_logger.end_operation(tool_trace, status="failed", error=error_msg, result=results)
            # Return False for failure, along with error info
            return False, results

    def generate_transformation_code(
        self,
        source_table: str,
        transformation_type: str,
        context: Optional[Dict] = None
    ) -> GeneratedCode:
        # This method might need rethinking or removal if generate_from_nl covers all cases.
        # For now, update it to use prefilling and remove metadata.
        request = f"Transform table {source_table} using {transformation_type}"
        if context:
            request += f" with context: {json.dumps(context)}"

        full_code = self._cached_generate_prefilled(request) # Use new helper

        # Define basic validation checks that operate on the 'df' passed to the validation scope
        validation_checks = [
            "assert df is not None",
            "assert isinstance(df, DataFrame)"
        ]
        # Check if result_df exists and is a DataFrame - THIS IS DONE BY THE EXECUTOR NOW
        # core_check_1 = "assert 'result' in locals(), \"'result' variable not found in generated code.\""
        # core_check_2 = "assert isinstance(result, DataFrame), f\"'result' is type {type(result).__name__}, not DataFrame.\""
        # validation_checks = [core_check_1, core_check_2]

        return GeneratedCode(
            code=full_code, # Already includes prelude
            description=f"Code for {transformation_type} on {source_table}.", # Placeholder
            tables_used=[source_table], # Basic assumption
            estimated_complexity="unknown", # Placeholder
            validation_checks=validation_checks
        )


    def regenerate_with_context(
        self,
        original_request: str,
        previous_code: str, # Note: previous_code might include the prelude now
        error_message: str,
        analysis: str
    ) -> GeneratedCode:
        """
        Regenerate Spark code using context and prefilling.
        Bypasses caching for regeneration attempts.
        """
        full_code = "" # Initialize full_code
        try:
            # Step 1: Regenerate the code using prefilling
            system_prompt, user_prompt = self._construct_regenerate_prompt(
                original_request=original_request,
                previous_code=previous_code, # Pass the potentially full previous code
                error_message=error_message,
                analysis=analysis
            )
            # Regeneration bypasses caching (cache_key=None)
            full_code = self._generate_code_with_prompt(system_prompt, user_prompt, cache_key=None)

            if not full_code or not isinstance(full_code, str):
                 raise ValueError("Prefilled code regeneration failed to return a valid string.")

            # Step 2: Assemble the GeneratedCode object (NO METADATA CALL)
            # Define basic validation checks that operate on the 'df' passed to the validation scope
            validation_checks = [
                "assert df is not None",
                "assert isinstance(df, DataFrame)"
            ]
            # core_check_1 = "assert 'result' in locals(), \"'result' variable not found in regenerated code.\""
            # core_check_2 = "assert isinstance(result, DataFrame), f\"'result' is type {type(result).__name__}, not DataFrame.\""
            # validation_checks = [core_check_1, core_check_2]

            final_generated_code = GeneratedCode(
                code=full_code, # Already includes prelude
                description="Code regenerated via prefilling after error.", # Placeholder
                tables_used=[], # Placeholder
                estimated_complexity="unknown", # Placeholder
                validation_checks=validation_checks
            )
            logger.info(f"Successfully regenerated prefilled code object: {final_generated_code!r}")
            return final_generated_code

        except Exception as e:
            logger.error("Failed to regenerate code with context using prefilling", exc_info=True)
            error_detail = f"Prefilled code regeneration failed. Code generated before error (if any):\n---\n{full_code}\n---"
            raise RuntimeError(error_detail) from e


# Create a singleton instance for global use.
db_config_for_init = {
    'dbname': os.environ.get("POSTGRES_DB"),
    'user': os.environ.get("POSTGRES_USER"),
    'password': os.environ.get("POSTGRES_PASSWORD"),
    'host': os.environ.get("POSTGRES_HOST", "localhost"),
    'port': os.environ.get("POSTGRES_PORT", "5432")
}
print(f"DEBUG: Initializing CodeGenerator with db_config: {db_config_for_init}")
generator = CodeGenerator(db_config_for_init)