# PheBee

**PheBee** is a phenotype-to-cohort query service that integrates structured biomedical ontologies and AWS-native infrastructure to support translational research. It enables researchers and clinicians to ask complex questions about phenotypic data in patient cohorts, such as:

- "Which subjects have a specific phenotype or any of its descendants?"
- "How frequently does a phenotype occur within a cohort?"

PheBee leverages ontologies like HPO, MONDO, OMIM, and Orphanet to provide deep, hierarchical querying. It integrates with in-house tools such as Mr. Phene and supports knowledge graph-enhanced retrieval for LLM-powered chat agents.

## Features

- Query patient cohorts based on ontological relationships
- Discover disease-phenotype associations
- Graph-based data storage in AWS Neptune
- RESTful API with OpenAPI spec
- Serverless architecture powered by AWS SAM and Lambda
- Integration with SPARQL, DynamoDB, and S3
- Automated deployment and testing workflows

---

## Getting Started

### Configuration Setup

This project provides a `samconfig.yaml.example` file as a template for your deployment configuration.

To get started, copy it to create your own `samconfig.yaml`:

```bash
cp samconfig.yaml.example samconfig.yaml
```

Then, edit the file with your environment-specific values. Each section of the file contains deployment settings for a  stack in a given environment. Here's what each field means:

```yaml
prod:                                  # The environment name
  deploy:
    parameters:
      stack_name: phebee-prod          # The name of the CloudFormation stack to be created or updated
      capabilities:
        - CAPABILITY_IAM               # Allows creation of IAM resources
        - CAPABILITY_NAMED_IAM         # Allows creation of named IAM roles and policies
      parameter_overrides:
        - VpcId=                       # The ID of your target VPC, which allows your Lambda functions and resources to connect securely within your private network
                                # Learn more: https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html
        - SubnetId1=                   # The first subnet ID, typically in the same availability zone as other services your app needs to access
                                # Learn more: https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Subnets.html
        - SubnetId2=                   # The second subnet ID, usually in a different availability zone for high availability and fault tolerance
                                # Learn more: https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Subnets.html
      tags:
        - app=phebee                   # Tags applied to the stack for resource tracking or cost management
```

Once filled in, this configuration will allow you to run:

```bash
sam deploy --config-env prod
```

This command will use the parameters defined in your `samconfig.yaml` without needing to specify them manually each time.

### Prerequisites

Before building or deploying PheBee, make sure you have:

- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) installed and configured
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- Python 3.9+
- `pip` and `virtualenv` (recommended)
- AWS credentials with appropriate IAM permissions for deploying a SAM app

To install the required Python dependencies for deployment:

```bash
pip install awscli aws-sam-cli
```

Then configure AWS:

```bash
aws configure
```

---

## Building and Deploying PheBee

You can manually build and deploy the SAM application using the AWS SAM CLI.

### 1. Build the SAM Application

```bash
sam build
```

This command compiles the application and its dependencies into `.aws-sam/build`.

### 2. Deploy the Application

```bash
sam deploy --config-env dev \
           --no-confirm-changeset \
           --resolve-s3 \
           --no-fail-on-empty-changeset
```

Optional flags:

- `--profile <your-profile>`: Use a named AWS profile
- `--stack-name <custom-stack>`: Deploy under a custom stack name

To check deployment status:

```bash
aws cloudformation describe-stacks --stack-name <your-stack-name>
```

### 3. Clean Up Resources

```bash
sam delete --stack-name <your-stack-name> --no-prompts
```

---

## Running Integration Tests

Integration tests validate the infrastructure and APIs by deploying the stack and exercising key endpoints.

### Prerequisites

Install dependencies:

```bash
pip install pytest boto3
```

Ensure your AWS credentials are configured (`aws configure`).

### Run All Integration Tests

```bash
pytest -m integration -v
```

With profile or environment:

```bash
pytest -m integration --profile=dev --config-env=dev -v
```

Use an existing deployed stack:

```bash
pytest -m integration --existing-stack <your-stack-name> -v
```

Run a specific test:

```bash
pytest tests/integration/test_cloudformation_stack.py::test_cloudformation_stack -m integration -v
```

---

## Project Structure

```
├── LICENSE                       # Project license
├── README.md                     # This README file
├── api.yaml                      # OpenAPI specification for the PheBee REST API
├── functions/                    # Lambda function definitions
├── model/                        # Data models and ontology loading logic
├── statemachine/                 # AWS Step Function definitions
├── src/                          # Supporting Python modules and utilities
├── tests/                        # Unit and integration tests
├── pyproject.toml                # Python project configuration
├── pytest.ini                    # Pytest configuration file
├── samconfig.yaml.example        # Sample SAM config for customization
├── setup.cfg                     # Additional project setup config
├── template.yaml                 # AWS SAM template
├── uv.lock                       # Dependency lock file (used by uv or pip-tools)
```

---

## Contributing

We welcome contributions! Please open an issue or submit a pull request for bug reports, feature suggestions, or general improvements.

---

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](./LICENSE) file for details.

---

## References

- [AWS SAM Documentation](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html)
- [Pytest Documentation](https://docs.pytest.org/en/latest/)
- [HPO Ontology](https://hpo.jax.org/)
- [SPARQL Specification](https://www.w3.org/TR/sparql11-query/)

