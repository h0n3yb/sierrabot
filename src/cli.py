#!/usr/bin/env python3
"""
Command-Line Interface for the Autonomous Transformation Agent.

Allows users to submit transformation requests directly from the terminal.
"""

import click
import logging
import os
import sys
from pprint import pprint
from typing import Optional
from tabulate import tabulate
from src.agent_loop import AgentLoop, TransformationStatus
from src.code_generator import generator
from src.logger import agent_logger

# Ensure the src directory is in the Python path for imports
# This assumes the script is run from the project root (e.g., python src/cli.py ...)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from src.agent_loop import agent, TransformationStatus
except ImportError as e:
    print(f"Error: Failed to import agent components. Make sure you run this script from the project root.", file=sys.stderr)
    print(f"Details: {e}", file=sys.stderr)
    sys.exit(1)

# Set up basic logging first
logger = logging.getLogger(__name__)

# Suppress py4j debug messages
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)

agent = AgentLoop()

@click.command()
@click.argument('request_text', type=str)
@click.option('--use-tool', is_flag=True, default=False, help='Enable schema exploration tool for analysis.')
@click.option('--viz', is_flag=True, default=False, help='Display results in a table.')
@click.option('--debug', is_flag=True, default=False, help='Enable INFO level logging.')
def transform_cli(request_text: str, use_tool: bool, viz: bool, debug: bool):
    """Processes a natural language TRANSFORMATION_REQUEST via the agent loop."""
    
    # Configure logging level based on --debug flag
    log_level = logging.INFO if debug else logging.WARNING
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s' # Added logger name
    # Use force=True to allow reconfiguration
    logging.basicConfig(level=log_level, format=log_format, force=True) 
    # Set root logger level, basicConfig might not be enough if libraries use getLogger directly
    logging.getLogger().setLevel(log_level) 
    
    logger.info(f"Logging level set to: {logging.getLevelName(log_level)}") # Log the level being used

    # Display Banner
    banner = r"""
  ______ _ _______ ______  ______  _______ 
 / _____) (_______|_____ \(_____ \(_______)
( (____ | |_____   _____) )_____) )_______ 
 \____ \| |  ___) |  __  /|  __  /|  ___  |
 _____) ) | |_____| |  \ \| |  \ \| |   | |
(______/|_|_______)_|   |_|_|   |_|_|   |_|
                                           
    """
    click.secho(banner, fg="yellow")
    
    if not request_text or not request_text.strip():
        click.echo("Error: Transformation request cannot be empty.", err=True)
        sys.exit(1)
        
    click.echo(f"Processing request: '{request_text}'")
    click.echo(f"Schema Exploration Tool Enabled: {use_tool}")
    click.echo("---")
    
    try:
        # Call the agent loop
        result = agent.process_request(request_text, use_schema_tool=use_tool)
        
        # Display results
        click.echo(f"Status: {result.status.value}")
        click.echo(f"Message: {result.message}")
        click.echo(f"Retries: {result.retry_count}")
        
        if result.status == TransformationStatus.SUCCESS:
            click.echo("---")
            click.echo("Result Data:")
            # Display result as a table if data exists
            result_data = result.data.get('result') if result.data else None
            if result_data and isinstance(result_data, list) and len(result_data) > 0:
                if viz:
                    click.echo(tabulate(result_data, headers="keys", tablefmt="psql"))
                else:
                    pprint(result_data)
            elif result_data is not None: # Handle empty list case
                click.echo("(Result data is empty)")
            else:
                click.echo("(No data returned)")
            
            # Ask for confirmation
            click.echo("---")
            
            # Stop the spinner before asking for user input
            agent_logger.stop_display()
            try:
                if click.confirm('Do you accept this result?'):
                    click.echo("Result accepted.")
                else:
                    reason = click.prompt("Please provide a reason for rejection")
                    agent_logger.warning("User rejected successful result", metadata={"reason": reason, "original_request": request_text})
                    click.echo("--- Attempting regeneration based on feedback --- ")
                    
                    # --- Trigger Regeneration --- 
                    # NOTE: Display will be restarted before regeneration starts
                    try:
                        # We don't easily have the successful code here, so pass placeholders
                        # Rely on the user reason and analysis for context
                        analysis_for_regen = f"User rejected the previous output for the following reason: {reason}. Please generate a new version addressing the feedback."
                        error_for_regen = f"User Rejection: {reason}"
                        
                        # Restart display *before* potentially long regeneration
                        agent_logger.start_display()
                        regenerated_code_obj = generator.regenerate_with_context(
                            original_request=request_text,
                            previous_code="# User rejected previous successful code", # Placeholder
                            error_message=error_for_regen,
                            analysis=analysis_for_regen
                        )
                        
                        click.echo("Regenerated code, attempting execution...")
                        # Execute the *new* code (need executor import)
                        from src.spark_executor import executor # Import locally for now

                        # Stop display again for potential Spark output/errors
                        agent_logger.stop_display()
                        try:
                            is_valid, new_result_df, error_msg = executor.execute(
                                regenerated_code_obj.code,
                                regenerated_code_obj.validation_checks
                            )
                        finally:
                            # Restart display after execution attempt
                            agent_logger.start_display()
                        
                        click.echo("---")
                        if is_valid and not error_msg:
                            click.secho("✓ Regenerated code executed successfully!", fg="green")
                            click.echo("New Result Data:")
                            if new_result_df is not None:
                                # Convert Pandas DataFrame to list of dicts for tabulate
                                data_dicts = new_result_df.limit(100).toPandas().to_dict('records')
                                if data_dicts:
                                    if viz:
                                        click.echo(tabulate(data_dicts, headers="keys", tablefmt="psql"))
                                    else:
                                        pprint(data_dicts)
                                else:
                                    click.echo("(Result data is empty)")
                            else:
                                click.echo("(No data returned from regenerated code)")
                        else:
                            click.secho("✗ Regenerated code execution failed!", fg="red")
                            click.echo(f"Error: {error_msg or 'Validation failed'}", err=True)

                    except Exception as regen_e:
                        logger.error("Error during user-triggered regeneration or execution", exc_info=True)
                        click.echo(f"Error during regeneration: {regen_e}", err=True)
            finally:
                # Ensure the spinner display is restarted after user input
                # unless regeneration was triggered (it restarts before that)
                if 'regenerated_code_obj' not in locals(): 
                    agent_logger.start_display()
        elif result.error:
            click.echo("---")
            click.echo(f"Error Details: {result.error}", err=True)
            
    except Exception as e:
        logger.error("An unexpected error occurred during CLI processing", exc_info=True)
        click.echo(f"\n--- UNEXPECTED ERROR ---", err=True)
        click.echo(f"An internal error occurred: {e}", err=True)
        sys.exit(1)
    finally:
        # Ensure Spark resources are cleaned up if the agent has a cleanup method
        if hasattr(agent, 'cleanup'):
            try:
                agent.cleanup()
                logger.info("Agent resources cleaned up.")
            except Exception as cleanup_e:
                logger.error(f"Error during agent cleanup: {cleanup_e}")

@click.command()
def logs():
    """Display recent logs"""
    trace = agent_logger.start_operation(
        "cli_view_logs",
        tags=["cli", "logs"]
    )
    
    try:
        # Read the last 20 lines from the human-readable log
        with open('logs/app.log', 'r') as f:
            logs = f.readlines()[-20:]
        
        click.echo("\nRecent Logs:")
        for log in logs:
            click.echo(log.strip())
        
        agent_logger.end_operation(
            trace,
            status="completed",
            result={"logs_displayed": len(logs)}
        )
        
    except Exception as e:
        agent_logger.error(
            "Failed to display logs",
            metadata={"error": str(e)},
            tags=["cli", "error"]
        )
        agent_logger.end_operation(trace, status="failed")
        click.secho(f"Error displaying logs: {str(e)}", fg="red")

if __name__ == '__main__':
    transform_cli() 