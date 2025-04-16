#!/usr/bin/env python3

import logging
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union, List
from agenttrace import TraceManager
from pythonjsonlogger import jsonlogger
import threading
import sqlite3

# Configure logging levels
logging.basicConfig(level=logging.INFO)

# Suppress py4j debug messages
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)

# --- Monkey-patch sqlite3.connect --- START ---
# Store the original connect function
original_sqlite_connect = sqlite3.connect

def patched_sqlite_connect(*args, **kwargs):
    """Patched version of connect that forces check_same_thread=False."""
    kwargs['check_same_thread'] = False
    # Call the original function with modified kwargs
    return original_sqlite_connect(*args, **kwargs)

# Apply the patch
sqlite3.connect = patched_sqlite_connect
logging.info("Applied monkey-patch to sqlite3.connect for thread safety.")
# --- Monkey-patch sqlite3.connect --- END ---

# Initialize thread local storage
thread_local = threading.local()

class ThreadSafeTraceManager:
    """Thread-safe wrapper for AgentTrace"""
    def __init__(self, db_path: str, colored_logging: bool = True):
        self.db_path = db_path
        self.colored_logging = colored_logging
    
    @property
    def tracer(self):
        if not hasattr(thread_local, "tracer"):
            thread_local.tracer = TraceManager(
                db_path=self.db_path,
                colored_logging=self.colored_logging
            )
            agent_logger.debug(f"Initialized TraceManager for thread {threading.get_ident()}")
        return thread_local.tracer

# Initialize thread-safe tracer
tracer = ThreadSafeTraceManager(
    db_path=os.getenv("AGENTTRACE_DB_PATH", "logs/traces.db"),
    colored_logging=True
)

class AgentLogger:
    """
    Enhanced logger class that provides both human-readable and structured logging,
    with local AgentTrace integration for monitoring and debugging.
    """
    
    def __init__(self, name: str = "agent"):
        self.name = name
        self.logger = logging.getLogger(name)
        
        # Ensure logs directory exists
        self.logs_dir = Path(os.getenv("LOG_FILE_PATH", "logs/sierra.log")).parent
        self.logs_dir.mkdir(exist_ok=True)
        
        # Set log file path
        self.log_file = os.getenv("LOG_FILE_PATH", "logs/sierra.log")
        
        # Remove any existing handlers
        self.logger.handlers = []
        
        # Create formatters
        json_formatter = jsonlogger.JsonFormatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s',
            rename_fields={'asctime': 'timestamp'}
        )
        
        # File handler with JSON formatting
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(json_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler with human-readable format for development
        if os.getenv('ENVIRONMENT') == 'development':
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
    
    def _log(self, level: str, message: str, metadata: Optional[Dict[str, Any]] = None, 
             tags: Optional[List[str]] = None, session_id: Optional[str] = None,
             exc_info: bool | Exception | None = None) -> None:
        """Log a message with the specified level and additional context."""
        if metadata is None:
            metadata = {}
        if tags is None:
            tags = []
            
        # Prepare the extra fields for the JSON formatter
        # We don't put exc_info here as the logger handles it directly
        extra = {
            'metadata': metadata, # Keep metadata separate for JSON
            'tags': tags,          # Keep tags separate for JSON
            'session_id': session_id # Keep session_id separate for JSON
        }
        
        # Get the numeric level
        level_number = getattr(logging, level.upper())
        
        # Log the message with extra context and exc_info
        # The standard logger knows how to handle exc_info=True or an exception instance
        self.logger.log(level_number, message, extra=extra, exc_info=exc_info)
    
    def start_operation(self, operation: str, metadata: Optional[Dict[str, Any]] = None,
                       tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """Start a new traced operation with enhanced metadata"""
        trace_data = {
            "operation": operation,
            "metadata": {
                **(metadata or {}),
                "start_time": datetime.utcnow().isoformat(),
                "project": os.getenv("AGENTTRACE_PROJECT_ID", "sierra"),
                "environment": os.getenv("AGENTTRACE_ENV", "development")
            },
            "tags": [*(tags or []), "autonomous_transformer"]
        }
        
        # Start AgentTrace trace using thread-safe tracer
        session_id = tracer.tracer.add_trace(
            "START",
            operation,
            args=trace_data["metadata"],
            tags=trace_data["tags"]
        )
        
        trace_data["session_id"] = session_id
        
        self.logger.info(
            f"Starting operation: {operation}",
            extra={
                'metadata': trace_data["metadata"],
                'tags': trace_data["tags"],
                'session_id': session_id
            }
        )
        
        return trace_data
    
    def end_operation(self, trace_data: Dict[str, Any], status: str = "completed",
                     result: Optional[Dict[str, Any]] = None,
                     error: Optional[str] = None,
                     duration_ms: Optional[float] = None) -> None:
        """End a traced operation with performance metrics"""
        # Calculate duration if not provided
        if duration_ms is None and trace_data.get("metadata", {}).get("start_time"):
            try:
                start_time = datetime.fromisoformat(trace_data["metadata"]["start_time"])
                duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            except (KeyError, ValueError):
                duration_ms = 0.0  # Fallback if we can't calculate duration
        
        metadata = {
            **(trace_data.get("metadata", {})),
            "result": result,
            "status": status,
            "error": error,
            "end_time": datetime.utcnow().isoformat(),
            "duration_ms": duration_ms or 0.0  # Ensure duration is never None
        }
        
        # End AgentTrace trace with complete data using thread-safe tracer
        tracer.tracer.add_trace(
            "END",
            trace_data["operation"],
            result=result,
            duration=duration_ms or 0.0,  # Ensure duration is never None
            tags=[*trace_data.get("tags", []), f"status_{status}"],
            session_id=trace_data.get("session_id")
        )
        
        self.logger.info(
            f"Ending operation: {trace_data['operation']} ({status})",
            extra={
                'metadata': metadata,
                'tags': trace_data.get("tags", []),
                'session_id': trace_data.get("session_id")
            }
        )
    
    def info(self, message: str, metadata: Optional[Dict[str, Any]] = None, tags: Optional[List[str]] = None, session_id: Optional[str] = None) -> None:
        """Log an info message with enhanced context"""
        self._log("INFO", message, metadata, tags, session_id)
    
    def error(self, message: str, metadata: Optional[Dict[str, Any]] = None, 
              tags: Optional[List[str]] = None, session_id: Optional[str] = None,
              exc_info: bool | Exception | None = None) -> None:
        """Log an error message with enhanced context and optional traceback."""
        # Pass exc_info to the internal _log method
        self._log("ERROR", message, metadata, [*(tags or []), "error"], session_id, exc_info=exc_info)
    
    def warning(self, message: str, metadata: Optional[Dict[str, Any]] = None, 
                tags: Optional[List[str]] = None, session_id: Optional[str] = None,
                exc_info: bool | Exception | None = None) -> None:
        """Log a warning message with enhanced context and optional traceback."""
        self._log("WARNING", message, metadata, [*(tags or []), "warning"], session_id, exc_info=exc_info)
    
    def debug(self, message: str, metadata: Optional[Dict[str, Any]] = None, tags: Optional[List[str]] = None, session_id: Optional[str] = None) -> None:
        """Log a debug message with enhanced context"""
        # Typically don't need exc_info for debug, but could add if needed
        self._log("DEBUG", message, metadata, [*(tags or []), "debug"], session_id)

    def stop_display(self) -> None:
        """Stop the AgentTrace spinner display."""
        try:
            tracer.tracer.stop_spinner_display()
            self.debug("AgentTrace display stopped.")
        except Exception as e:
            self.error(f"Failed to stop AgentTrace display: {e}", exc_info=True)

    def start_display(self) -> None:
        """Start the AgentTrace spinner display."""
        try:
            tracer.tracer.start_spinner_display()
            self.debug("AgentTrace display started.")
        except Exception as e:
            self.error(f"Failed to start AgentTrace display: {e}", exc_info=True)

# Create a singleton instance
agent_logger = AgentLogger()

# Optional: Setup evaluation framework
def setup_evaluator(name: str, eval_data: list, task_func: callable, scoring_funcs: list):
    """Setup an AgentTrace evaluator for specific tasks"""
    from agenttrace import TracerEval
    return TracerEval(
        name=name,
        data=lambda: eval_data,
        task=task_func,
        scores=scoring_funcs
    ) 