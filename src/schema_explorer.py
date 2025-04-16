#!/usr/bin/env python3

"""
Schema Explorer Module

Provides intelligent, progressive database schema exploration with clean, atomic operations.
Built around the principle of minimal, need-based schema discovery.

Key concepts:
- Each method has a single responsibility
- Results are cached at appropriate levels
- All operations are read-only
- Exceptions are handled gracefully
- Everything is well-typed and documented
"""

import logging
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
import time
from functools import lru_cache
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor
import json
from .logger import agent_logger # Import agent_logger

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class ColumnInfo:
    """Immutable container for column metadata"""
    name: str
    data_type: str
    is_nullable: bool
    default_value: Optional[str]
    max_length: Optional[int]
    is_primary_key: bool = False
    foreign_key_reference: Optional[Tuple[str, str]] = None  # (table, column)

@dataclass(frozen=True)
class TableStats:
    """Statistical information about a table"""
    row_count: int
    avg_row_length: int
    has_index: bool
    last_analyzed: Optional[str]
    sample_values: Optional[Dict[str, List[Any]]] = None

class SchemaExplorer:
    """
    Intelligent database schema explorer that provides atomic, cached access to schema information.
    Designed for progressive schema discovery based on specific needs.
    """
    
    # Schemas to generally ignore
    SYSTEM_SCHEMAS = {'pg_catalog', 'information_schema', 'pg_toast'}
    
    def __init__(self, connection_params: Dict[str, str], cache_ttl: int = 300):
        """
        Initialize the schema explorer with connection parameters and cache settings.
        
        Args:
            connection_params: Database connection parameters
            cache_ttl: Cache time-to-live in seconds (default: 5 minutes)
        """
        self.connection_params = connection_params
        self.cache_ttl = cache_ttl
        self._accessible_schemas_cache: Optional[Set[str]] = None
        self._table_cache: Dict[str, float] = {}  # qualified_table_name -> last_refresh_time
        self._column_cache: Dict[str, Dict[str, ColumnInfo]] = {}  # qualified_table_name -> {column_name -> info}
        self._stats_cache: Dict[str, TableStats] = {}  # qualified_table_name -> stats
        
    def _get_connection(self) -> connection:
        """Create a read-only connection to PostgreSQL"""
        return psycopg2.connect(
            **self.connection_params,
            cursor_factory=RealDictCursor,
            options="-c default_transaction_read_only=on"
        )

    @lru_cache(maxsize=1)
    def _get_accessible_schemas(self) -> Set[str]:
        """Get schemas accessible by the current user, excluding system schemas."""
        # Check internal cache first (lru_cache handles the actual call caching)
        if self._accessible_schemas_cache is not None:
            return self._accessible_schemas_cache
        
        accessible_schemas = set()
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Query schemas the user has USAGE permission on
                    cur.execute("""
                        SELECT schema_name 
                        FROM information_schema.schemata
                        WHERE has_schema_privilege(current_user, schema_name, 'USAGE')
                    """)
                    for row in cur.fetchall():
                        schema_name = row['schema_name']
                        if schema_name not in self.SYSTEM_SCHEMAS:
                            accessible_schemas.add(schema_name)
                            
            if not accessible_schemas:
                 logger.warning("No accessible non-system schemas found.")
            else:
                 logger.info(f"Found accessible schemas: {accessible_schemas}")
                 
            self._accessible_schemas_cache = accessible_schemas
            # No return here, finally block handles it
            
        except Exception as e:
            logger.error("Failed to fetch accessible schemas", exc_info=True)
            self._accessible_schemas_cache = set()
            # No return here, finally block handles it
        finally:
             # Ensure we return the cached value (which might be empty set on error)
             return self._accessible_schemas_cache if self._accessible_schemas_cache is not None else set()

    # Removed lru_cache here as it now depends on dynamic schemas
    def get_all_tables(self) -> Set[str]:
        """
        Get all fully qualified table names (schema.table) in accessible schemas.
        Results are implicitly cached by _get_accessible_schemas.
        """
        all_qualified_tables = set()
        accessible_schemas = self._get_accessible_schemas()
        
        if not accessible_schemas:
            return all_qualified_tables
            
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Use psycopg2 parameter substitution for list of schemas
                    schema_list = tuple(accessible_schemas)
                    query = """
                        SELECT table_schema, table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = ANY(%s)
                        AND table_type = 'BASE TABLE'
                    """
                    cur.execute(query, (list(schema_list),))
                    for row in cur.fetchall():
                        # Return as schema.table
                        all_qualified_tables.add(f"{row['table_schema']}.{row['table_name']}")
            return all_qualified_tables
        except Exception as e:
            logger.error("Failed to fetch tables for accessible schemas", exc_info=True)
            return set() # Return empty on error

    def get_column_info(self, qualified_table_name: str, column_name: Optional[str] = None) -> Dict[str, ColumnInfo]:
        """
        Get detailed information about one or all columns in a table.
        Results are cached per table with TTL.
        
        Args:
            qualified_table_name: Fully qualified name (schema.table)
            column_name: Optional specific column name
            
        Returns:
            Dict mapping column names to their metadata, or empty dict if table not found.
        """
        current_time = time.time()
        
        # Check cache freshness
        if (qualified_table_name not in self._column_cache or 
            current_time - self._table_cache.get(qualified_table_name, 0) > self.cache_ttl):
            try:
                self._refresh_table_metadata(qualified_table_name)
            except Exception as e:
                 logger.error(f"Error refreshing metadata for {qualified_table_name}, returning empty column info: {e}")
                 return {}
        
        # Handle case where table might not exist or metadata refresh failed
        if qualified_table_name not in self._column_cache:
            return {}
            
        if column_name:
            col_info = self._column_cache[qualified_table_name].get(column_name)
            return {column_name: col_info} if col_info else {}
        return self._column_cache[qualified_table_name]

    def _refresh_table_metadata(self, qualified_table_name: str) -> None:
        """
        Refresh all metadata for a specific qualified table name (schema.table).
        This includes column information, constraints, and relationships.
        """
        try:
            schema_name, table_name = qualified_table_name.split('.', 1)
        except ValueError:
            logger.error(f"Invalid qualified table name format: {qualified_table_name}")
            raise ValueError(f"Invalid qualified table name format: {qualified_table_name}")
            
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Get basic column information
                    cur.execute("""
                        SELECT 
                            column_name,
                            data_type,
                            is_nullable,
                            column_default,
                            character_maximum_length
                        FROM information_schema.columns 
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                    """, (schema_name, table_name))
                    columns = cur.fetchall()
                    
                    if not columns:
                         logger.warning(f"No columns found for {qualified_table_name}. Table might not exist or is inaccessible.")
                         self._column_cache[qualified_table_name] = {} # Cache empty info
                         self._table_cache[qualified_table_name] = time.time()
                         return
                         
                    # Get primary key information
                    cur.execute("""
                        SELECT c.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.constraint_column_usage AS ccu 
                        USING (constraint_schema, constraint_name)
                        JOIN information_schema.columns AS c 
                        ON c.table_schema = tc.constraint_schema
                        AND tc.table_name = c.table_name 
                        AND ccu.column_name = c.column_name
                        WHERE constraint_type = 'PRIMARY KEY'
                        AND tc.table_schema = %s AND tc.table_name = %s
                    """, (schema_name, table_name))
                    primary_keys = {row['column_name'] for row in cur.fetchall()}
                    
                    # Get foreign key information
                    cur.execute("""
                        SELECT
                            kcu.column_name,
                            ccu.table_schema AS foreign_table_schema, -- Get schema too
                            ccu.table_name AS foreign_table_name,
                            ccu.column_name AS foreign_column_name
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name
                        JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name AND ccu.constraint_schema = tc.constraint_schema
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = %s AND tc.table_name = %s
                    """, (schema_name, table_name))
                    foreign_keys = {
                        row['column_name']: (f"{row['foreign_table_schema']}.{row['foreign_table_name']}", row['foreign_column_name'])
                        for row in cur.fetchall()
                    }
                    
                    # Build column cache
                    current_table_cols = {}
                    for col in columns:
                        current_table_cols[col['column_name']] = ColumnInfo(
                            name=col['column_name'],
                            data_type=col['data_type'],
                            is_nullable=col['is_nullable'] == 'YES',
                            default_value=col['column_default'],
                            max_length=col['character_maximum_length'],
                            is_primary_key=col['column_name'] in primary_keys,
                            foreign_key_reference=foreign_keys.get(col['column_name'])
                        )
                    self._column_cache[qualified_table_name] = current_table_cols
                    self._table_cache[qualified_table_name] = time.time()
                    logger.info(f"Refreshed metadata for {qualified_table_name}")
                    
        except ValueError as ve: # Catch the specific error raised above
             logger.error(f"Invalid table format for metadata refresh: {qualified_table_name}: {ve}")
             # Re-raise or handle as needed - currently raised from split
             raise
        except Exception as e:
            # Standard log for other errors
            logger.error(f"Failed to refresh metadata for table {qualified_table_name}", exc_info=True)
            self._column_cache.pop(qualified_table_name, None)
            self._table_cache.pop(qualified_table_name, None)
            raise RuntimeError(f"Could not refresh table metadata for {qualified_table_name}: {str(e)}") from e

    def get_table_relationships(self, qualified_table_name: str) -> Dict[str, List[Dict[str, str]]]:
        """
        Get incoming and outgoing relationships for a table.
        Relationships now refer to qualified table names.
        
        Returns:
            Dict with 'incoming' and 'outgoing' relationship lists
        """
        try:
            schema_name, table_name = qualified_table_name.split('.', 1)
        except ValueError:
            raise ValueError(f"Invalid qualified table name format: {qualified_table_name}")
            
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Get outgoing foreign keys (points TO other tables)
                    cur.execute("""
                        SELECT
                            kcu.column_name as from_column,
                            ccu.table_schema as to_schema,
                            ccu.table_name as to_table,
                            ccu.column_name as to_column
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                            ON tc.constraint_name = kcu.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name
                        JOIN information_schema.constraint_column_usage AS ccu
                            ON ccu.constraint_name = tc.constraint_name AND ccu.constraint_schema = tc.constraint_schema
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = %s AND tc.table_name = %s
                    """, (schema_name, table_name))
                    outgoing = [
                        {
                            "from_column": row["from_column"],
                            "to_table": f"{row['to_schema']}.{row['to_table']}", # Qualified name
                            "to_column": row["to_column"]
                        }
                         for row in cur.fetchall()
                    ]
                    
                    # Get incoming foreign keys (points FROM other tables TO this one)
                    cur.execute("""
                        SELECT
                            tc.table_schema as from_schema,
                            tc.table_name as from_table,
                            kcu.column_name as from_column,
                            ccu.column_name as to_column
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                            ON tc.constraint_name = kcu.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name
                        JOIN information_schema.constraint_column_usage AS ccu
                            ON ccu.constraint_name = tc.constraint_name AND ccu.constraint_schema = tc.constraint_schema
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND ccu.table_schema = %s AND ccu.table_name = %s
                    """, (schema_name, table_name))
                    incoming = [
                         {
                            "from_table": f"{row['from_schema']}.{row['from_table']}", # Qualified name
                            "from_column": row["from_column"],
                            "to_column": row["to_column"]
                         }
                        for row in cur.fetchall()
                    ]
                    
                    return {
                        'outgoing': outgoing,
                        'incoming': incoming
                    }
                    
        except Exception as e:
            logger.error(f"Failed to get relationships for table {qualified_table_name}", exc_info=True)
            raise RuntimeError(f"Could not fetch table relationships: {str(e)}") from e

    def get_table_stats(self, qualified_table_name: str, sample_size: int = 5) -> TableStats:
        """
        Get statistical information about a table, including optional sample values.
        Results are cached with TTL.
        
        Args:
            qualified_table_name: Fully qualified name (schema.table)
            sample_size: Number of sample values to fetch per column
        """
        current_time = time.time()
        
        # Check cache freshness
        if (qualified_table_name not in self._stats_cache or 
            current_time - self._table_cache.get(qualified_table_name, 0) > self.cache_ttl):
            
            try:
                schema_name, table_name = qualified_table_name.split('.', 1)
            except ValueError:
                raise ValueError(f"Invalid qualified table name format: {qualified_table_name}")

            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cur:
                        # Use schema and table name in the query
                        cur.execute(""" 
                            SELECT 
                                pc.reltuples::bigint as row_count,
                                pg_total_relation_size(pc.oid) / 
                                    GREATEST(pc.reltuples, 1) as avg_row_length,
                                EXISTS (
                                    SELECT 1 FROM pg_index pi WHERE pi.indrelid = pc.oid
                                ) as has_index,
                                EXTRACT(EPOCH FROM psut.last_analyze) as last_analyzed
                            FROM pg_class pc
                            JOIN pg_namespace pn ON pn.oid = pc.relnamespace
                            LEFT JOIN pg_stat_user_tables psut 
                                ON psut.relid = pc.oid
                            WHERE pn.nspname = %s AND pc.relname = %s
                        """, (schema_name, table_name))
                        stats = cur.fetchone()
                        
                        if stats is None:
                            logger.warning(f"Could not fetch stats for table {qualified_table_name} from pg_class/pg_stat_user_tables.")
                            stats = {'row_count': 0, 'avg_row_length': 0, 'has_index': False, 'last_analyzed': None}
                        
                        # Get sample values if requested
                        sample_values = None
                        if sample_size > 0:
                            sample_values = {}
                            # Need qualified name for sampling query
                            columns = self.get_column_info(qualified_table_name)
                            for col_name in columns:
                                # Use qualified name in FROM clause
                                # Use psycopg2 quoting for safety
                                qualified_relation = cur.mogrify("%s.%s", (schema_name, table_name)).decode('utf-8')
                                qualified_column = cur.mogrify("%s", (col_name,)).decode('utf-8')
                                
                                try:
                                    # Ensure column name is also properly quoted if needed
                                    query = f"""
                                        SELECT DISTINCT {qualified_column}
                                        FROM {qualified_relation}
                                        WHERE {qualified_column} IS NOT NULL
                                        LIMIT %s
                                    """
                                    cur.execute(query, (sample_size,))
                                    sample_values[col_name] = [
                                        row[col_name] for row in cur.fetchall()
                                    ]
                                except Exception as sample_e:
                                     logger.warning(f"Could not get samples for column {qualified_table_name}.{col_name}: {sample_e}")
                                     sample_values[col_name] = ["<sample_error>"]
                        
                        self._stats_cache[qualified_table_name] = TableStats(
                            row_count=int(stats.get('row_count', 0) or 0),
                            avg_row_length=int(stats.get('avg_row_length', 0) or 0),
                            has_index=bool(stats.get('has_index', False)),
                            last_analyzed=str(stats.get('last_analyzed')) if stats.get('last_analyzed') else None,
                            sample_values=sample_values
                        )
                        self._table_cache[qualified_table_name] = current_time # Update main cache timestamp
                        
            except Exception as e:
                logger.error(f"Failed to get stats for table {qualified_table_name}", exc_info=True)
                raise RuntimeError(f"Could not fetch table statistics for {qualified_table_name}: {str(e)}") from e
        
        return self._stats_cache[qualified_table_name]

    def find_columns_by_type(self, data_type: str) -> Dict[str, List[str]]:
        """
        Find all columns of a specific data type across all accessible tables.
        
        Args:
            data_type: PostgreSQL data type to search for
            
        Returns:
            Dict mapping qualified table names to lists of matching column names
        """
        results: Dict[str, List[str]] = {}
        accessible_schemas = self._get_accessible_schemas()
        if not accessible_schemas:
            return results
            
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    schema_list = tuple(accessible_schemas)
                    query = """
                        SELECT table_schema, table_name, column_name
                        FROM information_schema.columns
                        WHERE table_schema = ANY(%s)
                        AND data_type = %s
                    """
                    cur.execute(query, (list(schema_list), data_type))
                    
                    for row in cur.fetchall():
                        qualified_table = f"{row['table_schema']}.{row['table_name']}"
                        if qualified_table not in results:
                            results[qualified_table] = []
                        results[qualified_table].append(row['column_name'])
                    return results
                    
        except Exception as e:
            logger.error(f"Failed to find columns of type {data_type}", exc_info=True)
            return {} # Return empty on error

    def suggest_related_tables(self, base_qualified_table: str, max_depth: int = 2) -> Dict[str, List[str]]:
        """
        Suggest related tables based on foreign key relationships up to a certain depth.
        Uses qualified table names.
        
        Args:
            base_qualified_table: Starting qualified table name (schema.table)
            max_depth: Maximum number of relationship hops to traverse
            
        Returns:
            Dict mapping hop distance to list of qualified table names
        """
        results: Dict[str, List[str]] = {str(i): [] for i in range(1, max_depth + 1)}
        visited = {base_qualified_table}
        current_depth = 1
        current_tables = {base_qualified_table}
        
        while current_depth <= max_depth and current_tables:
            next_tables = set()
            
            for table in current_tables:
                try:
                    relationships = self.get_table_relationships(table)
                except Exception as e:
                    logger.warning(f"Could not get relationships for {table} during suggestion: {e}")
                    continue # Skip table if relationships fail
                
                # Add all related tables at this depth
                for direction in ['incoming', 'outgoing']:
                    for rel in relationships[direction]:
                        # Relationships should already contain qualified names
                        related_table = rel['to_table' if direction == 'outgoing' else 'from_table']
                        if related_table not in visited:
                            results[str(current_depth)].append(related_table)
                            next_tables.add(related_table)
                            visited.add(related_table)
            
            current_tables = next_tables
            current_depth += 1
        
        return results

    def format_schema_for_llm(self, 
                            tables: Optional[List[str]] = None, 
                            include_relationships: bool = True,
                            include_samples: bool = False,
                            include_stats: bool = False) -> str:
        """
        Format schema information for specified or all accessible tables.
        Handles qualified table names.
        
        Args:
            tables: Optional list of specific QUALIFIED tables to include (schema.table)
            include_relationships: Whether to include relationship information
            include_samples: Whether to include sample values
            include_stats: Whether to include table statistics
            
        Returns:
            Formatted schema string
        """
        if tables is None:
            tables_to_format = list(self.get_all_tables())
        else:
            # Validate provided tables exist in accessible list
            all_accessible = self.get_all_tables()
            tables_to_format = [t for t in tables if t in all_accessible]
            if len(tables_to_format) != len(tables):
                 logger.warning(f"Some requested tables for LLM formatting are not accessible or don't exist: {set(tables) - set(tables_to_format)}")
        
        if not tables_to_format:
            return "No accessible tables found or specified."
            
        # Group tables by schema for better readability
        grouped_tables: Dict[str, List[str]] = {}
        for qualified_name in sorted(tables_to_format):
             try:
                 schema_name, table_name = qualified_name.split('.', 1)
                 if schema_name not in grouped_tables:
                     grouped_tables[schema_name] = []
                 grouped_tables[schema_name].append(qualified_name)
             except ValueError:
                 logger.warning(f"Skipping invalid qualified name during grouping: {qualified_name}")
        
        schema_parts = ["Database Schema:"]
        
        for schema_name, qualified_names in grouped_tables.items():
            schema_parts.append(f"\n--- Schema: {schema_name} ---")
            for qualified_table_name in qualified_names:
                try:
                    # Get basic table information using qualified name
                    columns = self.get_column_info(qualified_table_name)
                    if not columns:
                         schema_parts.append(f"\nTable: {qualified_table_name} (No column info available)")
                         continue # Skip if no columns found
                         
                    relationships = self.get_table_relationships(qualified_table_name) if include_relationships else None
                    
                    stats = None
                    if include_stats or include_samples:
                        try:
                            stats = self.get_table_stats(qualified_table_name, sample_size=3 if include_samples else 0)
                        except Exception as e:
                            logger.warning(f"Could not get stats/samples for table {qualified_table_name}: {e}")
                    
                    # Format table header
                    table_header = f"\nTable: {qualified_table_name}"
                    if stats and include_stats:
                        table_header += f" ({stats.row_count:,} rows)"
                    schema_parts.append(table_header)
                    
                    # Format columns
                    for col_name, col_info in columns.items():
                        type_str = col_info.data_type
                        if col_info.max_length:
                            type_str += f"({col_info.max_length})"
                        
                        constraints = []
                        if col_info.is_primary_key:
                            constraints.append("PK") # Abbreviate for LLM
                        if not col_info.is_nullable:
                            constraints.append("NOT NULL")
                        if col_info.foreign_key_reference:
                            ref_table, ref_col = col_info.foreign_key_reference
                            constraints.append(f"FK to {ref_table}({ref_col})")
                        
                        constraint_str = f" [{', '.join(constraints)}]" if constraints else ""
                        schema_parts.append(f"  - {col_name}: {type_str}{constraint_str}")
    
                    # Add sample values if requested and available
                    if include_samples and stats and stats.sample_values:
                        schema_parts.append("\n    Sample values:")
                        for col_name, samples in stats.sample_values.items():
                            if samples:
                                # Limit sample display length
                                sample_str = ", ".join(str(s)[:50] + ('...' if len(str(s)) > 50 else '') for s in samples)
                                schema_parts.append(f"    - {col_name}: {sample_str}")
                
                    # Add relationship information if requested
                    if include_relationships and relationships:
                        if relationships['incoming']:
                            refs = [f"{rel['from_table']}.{rel['from_column']}" for rel in relationships['incoming']]
                            schema_parts.append(f"\n    Referenced by: {', '.join(refs)}")
                        
                        if relationships['outgoing']:
                             refs = [f"{rel['to_table']}.{rel['to_column']} (on {rel['from_column']})" for rel in relationships['outgoing']]
                             schema_parts.append(f"\n    References: {', '.join(refs)}")
                             
                except Exception as e:
                    logger.warning(f"Could not format schema for table {qualified_table_name}: {e}", exc_info=True)
                    schema_parts.append(f"\nTable: {qualified_table_name} (Schema formatting failed)")
                
        return "\n".join(schema_parts)

    def get_distinct_column_values(self, qualified_table_name: str, column_name: str, limit: int = 50) -> List[Any]:
        """
        Get distinct values for a specific column in a table.

        Args:
            qualified_table_name: Fully qualified name (schema.table)
            column_name: The name of the column
            limit: Maximum number of distinct values to return (default: 50)

        Returns:
            List of distinct values.
        """
        # Consider adding caching here similar to other methods if performance becomes an issue.
        trace = agent_logger.start_operation(
            "get_distinct_column_values",
            metadata={
                "qualified_table_name": qualified_table_name,
                "column_name": column_name,
                "limit": limit
            },
            tags=["schema", "database", "distinct_values"]
        )
        
        try:
            schema_name, table_name = qualified_table_name.split('.', 1)
        except ValueError:
            error_msg = f"Invalid qualified table name format: {qualified_table_name}"
            agent_logger.end_operation(trace, status="failed", error=error_msg)
            raise ValueError(error_msg)

        results = []
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Use psycopg2's sql module for safe identifier quoting
                    from psycopg2 import sql
                    query = sql.SQL("""
                        SELECT DISTINCT {column}
                        FROM {table}
                        WHERE {column} IS NOT NULL
                        ORDER BY {column}
                        LIMIT %s
                    """).format(
                        column=sql.Identifier(column_name),
                        table=sql.Identifier(schema_name, table_name)
                    )
                    
                    logger.debug(f"Executing distinct values query: {cur.mogrify(query, (limit,)).decode('utf-8')}")
                    cur.execute(query, (limit,))
                    results = [row[column_name] for row in cur.fetchall()]
            
            agent_logger.end_operation(trace, status="completed", result={"distinct_values_count": len(results)})
            return results
        except Exception as e:
            error_msg = f"Failed to get distinct values for {qualified_table_name}.{column_name}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            agent_logger.end_operation(trace, status="failed", error=error_msg)
            # Return empty list on error, as it's informational data
            return []

# Example usage:
"""
explorer = SchemaExplorer({
    'dbname': 'your_database',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
})

# Get all tables
tables = explorer.get_all_tables()

# Get column information
columns = explorer.get_column_info('customers')

# Get relationships
relationships = explorer.get_table_relationships('orders')

# Get statistics
stats = explorer.get_table_stats('products')

# Find all timestamp columns
timestamp_columns = explorer.find_columns_by_type('timestamp')

# Get related tables
related = explorer.suggest_related_tables('customers', max_depth=2)

# Format schema for LLM
schema_str = explorer.format_schema_for_llm(
    tables=['customers', 'orders'],
    include_relationships=True,
    include_samples=True
)
""" 