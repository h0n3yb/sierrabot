# PySpark Code Generation Failure Modes and Solutions

This document outlines common failure modes encountered during the automatic generation of PySpark code from natural language requests within this project, specifically those addressed by the examples provided in the `CodeGenerator` system prompts (`src/code_generator.py`).

## 1. Ambiguous Column Reference in Joins

*   **Failure Mode:** `AnalysisException: Reference 'column_name' is ambiguous...`
*   **Cause:** Occurs when joining two DataFrames (either base tables or intermediate results) that share one or more column names (e.g., `id`, `name`, `customer_id`). Without explicit aliasing, Spark cannot determine which DataFrame's column is being referred to in join conditions or subsequent operations (`select`, `filter`, `groupBy`, etc.).
*   **Solution Demonstrated:**
    *   **Alias Both Sides:** Immediately alias *both* DataFrames involved in the join operation *before* the join call (e.g., `df1.alias('a').join(df2.alias('b'), ...)`). This applies to joins involving base tables and intermediate results.
    *   **Use Aliases Consistently:** Refer to columns using their respective aliases in the join condition (`F.col('a.id') == F.col('b.customer_id')`) and in *all* subsequent operations on the resulting joined DataFrame.

## 2. Missing Full Paths for Spark Components

*   **Failure Mode:** `AttributeError: 'function' object has no attribute 'Window'` or `NameError: name 'Window' is not defined` (or similar for other functions like `col`, `rank`, types like `StringType`).
*   **Cause:** The generated code snippet attempts to use Spark functions, classes, or types without their fully qualified path. While the standard prelude includes base imports (`import pyspark`), it does *not* include specific function/class imports (`from pyspark.sql.functions import col`, `from pyspark.sql.window import Window`). The generated code cannot rely on such imports existing within its scope.
*   **Solution Demonstrated:**
    *   **Use Explicit Paths:** Mandate the use of full, explicit paths for *all* PySpark components within the generated code block (e.g., `pyspark.sql.functions.col`, `pyspark.sql.window.Window`, `pyspark.sql.types.StringType`, `pyspark.sql.functions.rank`).

## 3. Incorrect Column Reference After Multiple Joins

*   **Failure Mode:** `AnalysisException: Cannot resolve column 'some_alias.original_alias.column'...` or similar column resolution errors.
*   **Cause:** After performing multiple joins and potentially multiple rounds of aliasing, attempting to access a column using an alias from an earlier stage of the transformation pipeline, rather than the alias assigned to the most recent intermediate DataFrame containing that column. Trying to "chain" aliases (e.g., `col('alias_step2.alias_step1.column')`) is incorrect.
*   **Solution Demonstrated:**
    *   **Reference Immediate Alias:** When selecting or filtering columns after a join, reference them using the alias assigned to the *immediate* DataFrame involved in that join or select operation (e.g., if `df_step2 = df_step1.alias('s1').join(...)`, refer to columns from `df_step1` as `col('s1.column_name')`).
    *   **Rename Ambiguous Columns:** Explicitly rename columns during `select` operations after joins, especially join keys or columns with common names, to avoid future ambiguity (e.g., `.select(F.col('a.id').alias('customer_id'), ...)`).

## 4. Nested Aggregation Functions

*   **Failure Mode:** `AnalysisException: It is not allowed to use an aggregate function in the argument of another aggregate function...` (or similar phrasing depending on Spark version).
*   **Cause:** Attempting to nest aggregate functions within a single `.agg()` call (e.g., `F.sum(F.count(...))`, `F.avg(F.sum(...))`). Spark requires aggregate operations to be performed sequentially.
*   **Solution Demonstrated:**
    *   **Multi-Step Aggregation:** Break down the nested aggregation into separate steps.
        1.  Perform the inner aggregation using `groupBy().agg()` and assign the result to a new intermediate DataFrame.
        2.  Optionally join this intermediate result back to other DataFrames if needed.
        3.  Perform the outer aggregation on the results of the first step using another `groupBy().agg()`. 