import pytest
from click.testing import CliRunner
from src.cli import cli, validate_transformation_request, ctx, TransformationContext

@pytest.fixture(autouse=True)
def reset_context():
    """Reset the CLI context before each test"""
    global ctx
    ctx.__init__()

def test_validate_transformation_request():
    """Test the validation of transformation requests"""
    # Test empty request
    assert not validate_transformation_request("")
    assert not validate_transformation_request("   ")
    
    # Test valid request
    assert validate_transformation_request("show all customers")

def test_transform_command():
    """Test the transform command"""
    runner = CliRunner()
    
    # Test with empty request
    result = runner.invoke(cli, ['transform', ''])
    assert result.exit_code == 0
    assert "Invalid transformation request" in result.output
    
    # Test with valid request
    result = runner.invoke(cli, ['transform', 'show all customers'])
    assert result.exit_code == 0
    assert "Processing transformation request" in result.output

def test_history_command():
    """Test the history command"""
    runner = CliRunner()
    result = runner.invoke(cli, ['history'])
    assert result.exit_code == 0
    assert "No previous transformations found" in result.output

def test_version_command():
    """Test the version command"""
    runner = CliRunner()
    result = runner.invoke(cli, ['version'])
    assert result.exit_code == 0
    assert "Autonomous Data Transformation Agent" in result.output 