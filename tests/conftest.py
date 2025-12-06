import pytest
import subprocess
import boto3
import uuid
from botocore.exceptions import ClientError


def pytest_addoption(parser):
    """
    Add custom command-line options to pytest.
    This will allow the use of an existing stack if the --existing-stack option is provided.
    """

    parser.addoption(
        "--existing-stack",
        action="store",
        default=None,
        help="Use an existing CloudFormation stack instead of creating a new one.",
    )

    parser.addoption(
        "--force-database-reset",
        action="store_true",
        default=False,
        help="Reset the Neptune database first when running tests on an existing stack.",
    )

    parser.addoption(
        "--profile",
        action="store",
        default=None,
        help="Use an AWS profile for session.",
    )

    parser.addoption(
        "--config-env",
        action="store",
        default="integration-test",
        help="Config environment name for the new stack. Default: integration-test.",
    )
    
    parser.addoption("--run-performance", action="store_true", help="Run performance tests")
    parser.addoption("--num-subjects", type=int, default=50, help="Number of subjects for performance tests")
    parser.addoption("--num-terms", type=int, default=1000, help="Number of terms/notes per subject for performance tests")
