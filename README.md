# Manually Deploying the SAM Application

This guide provides instructions on how to manually build and deploy the AWS SAM (Serverless Application Model) application using the AWS SAM CLI.

## Prerequisites

Ensure you have the following set up before deploying the SAM application:
- **AWS SAM CLI** installed.
- **AWS CLI** installed and configured with appropriate credentials.
- A valid **SAM template** (e.g., `template.yaml`) ready for deployment.

### Installation Commands (if needed)

To install the necessary dependencies:

```bash
pip install awscli aws-sam-cli
```

Configure your AWS CLI credentials:

```bash
aws configure
```

## Steps to Manually Deploy the SAM Application for development

### 1. Building the SAM Application

The `sam build` command compiles your SAM template and prepares the application for deployment. It also resolves any dependencies for your Lambda functions.

Run the following command:

```bash
sam build
```

This command will:
- Compile and bundle the application code and dependencies.
- Create a `.aws-sam/build/` folder with the packaged artifacts.

### 2. Deploying the SAM Application

After the build process is complete, you can deploy the application using the `sam deploy` command.

To deploy the application:

```bash
sam deploy --config-env dev \
           --no-confirm-changeset \
           --resolve-s3 \
           --no-fail-on-empty-changeset
```

If you need to deploy using a specific set of AWS credentials defined in your ~/.aws/config file, add the profile flag to the previous command:

```bash
           --profile <your-profile-name>
```

If you already have a development stack deployed and need to create an additional stack, add the stack name flag to the previous command:

```bash
           --stack-name <new-stack-name>
```

Explanation:
- **`--stack-name`**: The name of the CloudFormation stack that will be created or updated, e.g. phebee-dev.
- **`--capabilities`**: Required to allow SAM to create IAM roles and resources.
- **`--no-confirm-changeset`**: Skips the confirmation prompt, making the deployment non-interactive.
- **`--resolve-s3`**: Automatically creates an S3 bucket to store packaged artifacts if needed.
- **`--profile`**: Chooses a set of AWS credentials from the ~/.aws/config file

### 3. Viewing Stack Status

You can check the status of the deployed stack via the AWS Management Console under **CloudFormation** or by running the following AWS CLI command:

```bash
aws cloudformation describe-stacks --stack-name <your-stack-name>
```

### 4. Cleaning Up

After testing or using the SAM application, you can delete the stack to remove all associated AWS resources and avoid incurring costs. To delete the stack, use:

```bash
sam delete --stack-name <your-stack-name> --no-prompts
```

This will:
- Delete the CloudFormation stack.
- Remove all associated resources created during the deployment.

<h1>Running Integration Tests</h1>

This repository contains integration tests for deploying and managing AWS resources using the AWS SAM CLI and CloudFormation. These tests are marked as `@pytest.mark.integration` to allow for easy identification and execution of integration tests.

## Prerequisites

Ensure you have the following set up before running the integration tests:
- **AWS SAM CLI** installed.
- **AWS CLI** configured with appropriate credentials.
- **pytest** and **boto3** installed.

### Installation Commands (if needed)

```bash
pip install pytest boto3
```

Set up your AWS credentials using the AWS CLI:

```bash
aws configure
```

## Running the Integration Tests

### 1. Running All Integration Tests

To run all integration tests in the project, use the following command:

```bash
pytest -m integration -v
```

The --profile and config-env flags can also be used to specify AWS profiles for a new stack:

```bash
pytest -m integration --profile=dev --config-env=dev -v
```

This command will:
1. Build the SAM application using `sam build`.
2. Deploy the CloudFormation stack using `sam deploy`.
3. Run the integration tests marked with `@pytest.mark.integration`.
4. Automatically clean up the resources by deleting the CloudFormation stack after the tests.

If you already have a stack deployed and want to use it for integration tests, add the --existing-stack parameter to the pytest command.

```bash
pytest -m integration --existing-stack <your-existing-test-stack-name> -v
```

Note that the tests will potentially modify data in the stack, so it should not be used in situations where that is an issue.

### 2. Running a Specific Integration Test

You can run a specific integration test using the following command:

```bash
pytest tests/integration/test_cloudformation_stack.py::test_cloudformation_stack -m integration -v
```

This will run only the specified integration test, while skipping others.

### 3. Viewing Detailed Output

The `-v` flag enables verbose output, allowing you to see more detailed information about the tests being run, including the test names and execution status.

## How Tests Are Structured

- **Setup and Teardown**: The tests automatically deploy the necessary AWS resources before execution and clean them up afterward using the `sam delete` command.
- **Test Marking**: Integration tests are marked with `@pytest.mark.integration` to distinguish them from other types of tests (e.g., unit tests).
  
## Configuration

To avoid warnings related to unknown marks, ensure that the `integration` mark is registered in your `pytest.ini` file. This should already be set up, but you can add the following to `pytest.ini` if needed:

```ini
[pytest]
markers =
    integration: marks a test as an integration test
```

## Teardown and Cleanup

The integration tests automatically delete the CloudFormation stack after the tests finish. If you need to manually delete the stack, you can do so using the following command:

```bash
sam delete --stack-name <your-stack-name> --no-prompts
```

## Additional Resources

- [AWS SAM Documentation](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html)
- [Pytest Documentation](https://docs.pytest.org/en/latest/)
- [AWS CLI Documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](./LICENSE) file for details.