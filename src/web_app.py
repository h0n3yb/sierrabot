#!/usr/bin/env python3

from flask import Flask, render_template, request, jsonify
import logging
import os
from pathlib import Path
from pythonjsonlogger import jsonlogger
from .agent_loop import agent, TransformationStatus, AgentLoop
from .code_generator import generator
from .spark_executor import executor
from .self_correction import corrector
from typing import List, Dict
from datetime import datetime
import json
import pandas as pd
from .logger import agent_logger, tracer

# Set up basic logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress py4j debug messages
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)

class TransformationHistory:
    """Manages transformation history in memory and on disk"""
    def __init__(self, history_file: str = "logs/history.json"):
        self.history_file = history_file
        self.history: List[Dict] = self._load_history()
        
    def _load_history(self) -> List[Dict]:
        """Load history from file if it exists"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error("Failed to load history", exc_info=True)
        return []
    
    def _save_history(self):
        """Save history to file"""
        try:
            os.makedirs(os.path.dirname(self.history_file), exist_ok=True)
            with open(self.history_file, 'w') as f:
                json.dump(self.history, f, indent=2)
        except Exception as e:
            logger.error("Failed to save history", exc_info=True)
    
    def add_entry(self, request: str, result: Dict):
        """Add a new entry to the history"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'request': request,
            'result': result
        }
        self.history.append(entry)
        self._save_history()
    
    def get_entries(self, limit: int = 10) -> List[Dict]:
        """Get the most recent entries"""
        return sorted(
            self.history,
            key=lambda x: x['timestamp'],
            reverse=True
        )[:limit]
    
    def clear(self):
        """Clear the history"""
        self.history = []
        self._save_history()

# Get absolute paths and create directories
current_dir = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(current_dir, 'templates')
static_dir = os.path.join(current_dir, 'static')

Path(template_dir).mkdir(exist_ok=True, parents=True)
Path(static_dir).mkdir(exist_ok=True, parents=True)

# Initialize Flask app and history
app = Flask(__name__, 
           template_folder=template_dir,
           static_folder=static_dir)
history = TransformationHistory()

# Initialize Spark and agent in a way that prevents port conflicts
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'  # Force Spark to use localhost
os.environ['SPARK_UI_PORT'] = '4042'  # Use a different port for Spark UI

# Initialize agent after setting Spark environment variables
agent = AgentLoop()

logger.info("Flask application initialized")

# Add signal handlers to clean up resources
import signal
import atexit

def cleanup_handler(signum=None, frame=None):
    """Clean up resources before shutdown"""
    logger.info("Cleaning up resources...")
    try:
        # Clean up any Spark resources
        if hasattr(agent, 'cleanup'):
            agent.cleanup()
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
    finally:
        # Force exit after cleanup
        os._exit(0)

# Register cleanup handlers
atexit.register(cleanup_handler)
signal.signal(signal.SIGTERM, cleanup_handler)
signal.signal(signal.SIGINT, cleanup_handler)

# Store server instance globally for cleanup
server = None

@app.route('/')
def index():
    """Render the main dashboard page"""
    try:
        return render_template('index.html')
    except Exception as e:
        import traceback
        error_details = {
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        logger.error("Failed to render dashboard", exc_info=True)
        return jsonify({"error": "Failed to render dashboard", "details": error_details}), 500

@app.route('/api/transform', methods=['POST'])
def transform():
    """Handle transformation requests directly without confirmation step."""
    try:
        data = request.json
        if not data:
            logger.error("Invalid request data", extra={"data": data})
            return jsonify({"error": "Missing request data"}), 400

        instruction = data.get('instruction') or data.get('request')
        if not instruction:
            logger.error("Missing instruction in request", extra={"data": data})
            return jsonify({"error": "Missing 'instruction' or 'request' in request data"}), 400
            
        # Get the tool usage flag from the request, default to False if not provided
        use_schema_tool = data.get('use_schema_tool', False)

        logger.info(f"Processing transformation request: '{instruction}'")
        logger.info(f"Schema exploration tool enabled: {use_schema_tool}") # Log the flag
        
        # Call the agent loop directly (user_confirmed flag removed from agent.process_request)
        result = agent.process_request(instruction, use_schema_tool=use_schema_tool)
        
        response = {
            "status": result.status.value,
            "message": result.message,
            "data": None, # Initialize data as None
            "error": result.error,
            "retry_count": result.retry_count,
            "steps": [step.__dict__ for step in result.steps] # Include step details
        }
        
        # Preprocess result data for JSON serialization
        if result.data and 'result' in result.data and isinstance(result.data['result'], list):
            processed_result_list = []
            for row in result.data['result']:
                processed_row = {}
                for key, value in row.items():
                    # Handle potential array-like values first by converting to string
                    if isinstance(value, (list, tuple)): 
                        processed_row[key] = str(value)
                    elif pd.isna(value): # Check for NaN/NaT using pandas (now only on scalars)
                        processed_row[key] = None # Convert NaN/NaT to None (JSON null)
                    elif isinstance(value, datetime):
                        processed_row[key] = value.isoformat()
                    # Add more type checks if needed (e.g., for Decimal)
                    # elif isinstance(value, Decimal):
                    #     processed_row[key] = float(value) 
                    else:
                        # Attempt to convert other non-standard types or handle them
                        try:
                            # Check if it's serializable, otherwise convert to string
                            json.dumps({key: value})
                            processed_row[key] = value
                        except TypeError:
                            processed_row[key] = str(value) # Fallback to string
                processed_result_list.append(processed_row)
            # Create a copy of result.data and update the 'result' key
            response["data"] = result.data.copy() # Copy other potential keys like execution_time
            response["data"]['result'] = processed_result_list
        elif result.data: # Handle cases where data might not have the 'result' list structure
            response["data"] = result.data # Pass through other data structures
        
        # Determine appropriate HTTP status code
        status_code = 200 if result.status == TransformationStatus.SUCCESS else 422 # OK or Unprocessable Entity
            
        return jsonify(response), status_code
        
    except Exception as e:
        import traceback
        error_details = {
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        logger.error("Error processing transform request", exc_info=True)
        return jsonify({"error": "Internal server error", "details": error_details}), 500

@app.route('/api/history')
def get_history():
    """Return transformation history"""
    limit = request.args.get('limit', default=10, type=int)
    entries = history.get_entries(limit)
    return jsonify({'history': entries})

@app.route('/api/history/clear', methods=['POST'])
def clear_history():
    """Clear transformation history"""
    history.clear()
    return jsonify({'message': 'History cleared successfully'})

@app.route('/api/logs')
def get_logs():
    """Return recent logs for the dashboard"""
    try:
        # Read the last 100 lines from the log file
        try:
            with open('logs/app.log', 'r') as f:
                logs = f.readlines()[-100:]
        except FileNotFoundError:
            logs = []
        
        # Get traces from AgentTrace
        try:
            traces = tracer.tracer.get_traces(limit=100)
        except Exception as e:
            traces = []
            logger.warning("Failed to fetch traces", exc_info=True)
        
        return jsonify({
            "logs": logs,
            "traces": traces
        })
    except Exception as e:
        import traceback
        error_details = {
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        logger.error("Failed to fetch logs", exc_info=True)
        return jsonify({"error": "Failed to fetch logs", "details": error_details}), 500

@app.route('/api/traces', methods=['GET'])
def get_traces():
    """API endpoint to fetch traces"""
    try:
        limit = request.args.get('limit', default=100, type=int)
        offset = request.args.get('offset', default=0, type=int)
        session_id = request.args.get('session_id', type=str)
        
        # Get traces with pagination and optional session filtering
        traces = tracer.tracer.get_traces(
            limit=limit,
            session_id=session_id
        )
        
        # Apply offset
        traces = traces[offset:offset + limit]
        
        return jsonify({
            "traces": traces,
            "total": len(traces),
            "hasMore": len(traces) >= limit
        })
    except Exception as e:
        logger.error("Failed to fetch traces", exc_info=True)
        return jsonify({
            "error": "Failed to fetch traces",
            "details": str(e)
        }), 500

@app.route('/api/traces/<session_id>', methods=['GET'])
def get_trace_by_session(session_id):
    """API endpoint to fetch traces for a specific session"""
    try:
        traces = tracer.tracer.get_traces(session_id=session_id)
        return jsonify({
            "traces": traces,
            "total": len(traces)
        })
    except Exception as e:
        logger.error(f"Failed to fetch traces for session {session_id}", exc_info=True)
        return jsonify({
            "error": "Failed to fetch traces",
            "details": str(e)
        }), 500

@app.route('/api/sessions', methods=['GET'])
def get_sessions():
    """API endpoint to get unique session IDs"""
    try:
        # Get all traces
        traces = tracer.tracer.get_traces(limit=1000)  # Get a large number to find unique sessions
        
        # Extract unique session IDs and their metadata
        sessions = {}
        for trace in traces:
            session_id = trace.get('session_id')
            if session_id and session_id not in sessions:
                sessions[session_id] = {
                    'id': session_id,
                    'timestamp': trace.get('timestamp'),
                    'function_name': trace.get('function_name'),
                    'tags': json.loads(trace.get('tags', '[]')),
                    'trace_count': 1
                }
            elif session_id:
                sessions[session_id]['trace_count'] += 1
        
        # Convert to list and sort by timestamp
        session_list = list(sessions.values())
        session_list.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return jsonify({
            "sessions": session_list,
            "total": len(session_list)
        })
    except Exception as e:
        logger.error("Failed to fetch sessions", exc_info=True)
        return jsonify({
            "error": "Failed to fetch sessions",
            "details": str(e)
        }), 500

@app.after_request
def after_request(response):
    """Enable CORS"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

if __name__ == '__main__':
    logger.info("Starting Flask development server on port 5001...")
    try:
        app.run(host='0.0.0.0', port=5001, debug=True)
    except KeyboardInterrupt:
        cleanup_handler() 