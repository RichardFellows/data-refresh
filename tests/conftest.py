import pytest
import os
from unittest.mock import patch


@pytest.fixture(autouse=True)
def clean_env():
    """Clean environment variables before each test"""
    env_vars = ['SOURCE_DB_USER', 'SOURCE_DB_PASSWORD', 'TARGET_DB_USER', 'TARGET_DB_PASSWORD']
    
    # Store original values
    original_values = {var: os.environ.get(var) for var in env_vars}
    
    # Clear environment variables
    for var in env_vars:
        if var in os.environ:
            del os.environ[var]
    
    yield
    
    # Restore original values
    for var, value in original_values.items():
        if value is not None:
            os.environ[var] = value
        elif var in os.environ:
            del os.environ[var]