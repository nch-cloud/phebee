# PheBee

**PheBee** is a phenotype-to-cohort query service that integrates structured biomedical ontologies and AWS-native infrastructure to support translational research. It enables researchers and clinicians to ask complex questions about phenotypic data in patient cohorts, such as:

- "Which subjects have a specific phenotype or any of its descendants?"
- "How frequently does a phenotype occur within a cohort?"

PheBee leverages ontologies like HPO (Human Phenotype Ontology), MONDO (Monarch Disease Ontology), and ECO (Evidence and Conclusion Ontology) to provide deep, hierarchical querying and evidence classification.

## Features

- Query patient cohorts based on ontological relationships
- Graph-based data storage in AWS Neptune
- RESTful API with OpenAPI spec and AWS Signature V4 authentication
- Serverless architecture powered by AWS SAM and Lambda
- Iceberg tables registered in AWS Glue Data Catalog, enabling integration with Lake Formation and other analytics tools
- Scripted deployment (AWS SAM) and a pytest integration suite that can deploy and tear down a test stack

---

## Architecture

PheBee uses a hybrid architecture combining knowledge graphs with data lake technologies to enable both semantic reasoning and analytical queries at scale.

### Core Components

**AWS Neptune (Knowledge Graph)**
- Stores ontology hierarchies (HPO, MONDO, ECO) as RDF triples
- Enables SPARQL queries for ontological reasoning and relationship traversal

**Apache Iceberg (Data Lake)**
- Stores subject-term associations and clinical evidence as columnar data
- Queryable via AWS Athena for analytical workloads

**DynamoDB (Mappings, Version Registry and Cache)**
- Maps each project-scoped subject identifier to its shared internal subject ID
- Records installed ontology versions and their install timestamps
- Caches term-descendant lists per ontology version, populated on first query

**S3 (Object Storage)**
- Raw data staging for bulk imports (Phenopackets, NDJSON)
- Iceberg table storage (Parquet files)
- Ontology source files (OWL, OBO)

### Data Flow

1. **Ontology Loading**: OWL files → version-scoped Neptune named graph (e.g. `hpo~<version>`); for HPO and Mondo, OBO files → Iceberg ontology hierarchy table (ancestor closure per term); installed version recorded in DynamoDB
2. **Bulk Import**: S3 NDJSON batches → Step Functions orchestration → Iceberg evidence table → Neptune graph
3. **Materialization**: Evidence data is aggregated into dual-partitioned analytical tables for optimized query patterns
4. **Query Path**:
   - API Gateway → Lambda → Athena queries over the Iceberg subject-term and evidence tables
   - Descendant expansion reads the Iceberg ontology hierarchy table, cached per term and ontology version in DynamoDB
   - The HTTP API read operations do not query Neptune; the graph is written during ingestion and can be queried with SPARQL through the read-only SPARQL function (direct Lambda invocation only, not exposed through the HTTP API)

### Why This Architecture?

- **Semantic reasoning** requires graph traversal (Neptune)
- **Analytical queries** at scale need columnar storage (Iceberg/Athena)
- **Hybrid approach** gives best of both worlds: ontology intelligence + data lake performance
- **Serverless** components (Lambda, Step Functions) minimize operational overhead
- **Open formats** (RDF, Iceberg, Parquet) ensure data portability and interoperability

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
- Python 3.11 (the Lambda runtime; the shared layer does not import on Python 3.9)
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

### 3. Upload the EMR Scripts

The bulk import and rebuild state machines run their Spark jobs from `s3://<PheBeeBucketName>/scripts/`, which `sam deploy` does not populate. Upload them once after each deployment that changes `scripts/`, from the project root:

```bash
./utilities/deploy-scripts.sh <your-stack-name>
```

### 4. Clean Up Resources

```bash
sam delete --stack-name <your-stack-name> --no-prompts
```

---

## Running Integration Tests

Integration tests validate the infrastructure and APIs by deploying the stack and exercising key endpoints.

### Prerequisites

Install dependencies:

```bash
pip install pytest boto3 requests requests-aws4auth
```

Ensure your AWS credentials are configured (`aws configure`).

When no existing stack is given, the suite builds and deploys a new stack using the parameter overrides in `tests/integration/conftest.py`; review those for your account before relying on that path.

`tests/integration/test_reset_database.py` erases all data in the target stack. It runs against stacks the suite deploys for itself, and is skipped against an existing stack unless `PHEBEE_ALLOW_DATABASE_RESET=1` is set.

### Run All Integration Tests

```bash
pytest tests/integration -v
```

`pytest -m integration` selects only the modules that carry the `integration` marker, which is a subset of the suite.

With profile or environment:

```bash
pytest -m integration --profile=dev --config-env=dev -v
```

Use an existing deployed stack:

```bash
# Using command-line flag
pytest -m integration --existing-stack <your-stack-name> -v

# Or create .phebee-test-stack file for persistent configuration
echo "your-stack-name" > .phebee-test-stack
pytest -m integration -v
```

See [Testing Guide](tests/README.md#using-existing-stack) for details.

Run a specific test:

```bash
pytest tests/integration/test_create_project.py -v
```

---

## Performance Evaluation

PheBee includes comprehensive performance testing infrastructure to evaluate bulk data ingestion throughput and API query latency at scale with realistic clinical data patterns.

### Key Features

- **Realistic synthetic data generation** with disease clustering and clinical documentation patterns
- **Bulk import performance measurement** for large-scale data ingestion
- **API latency testing** with 7 query patterns representing real-world use cases
- **Reproducible benchmark datasets** (1K-100K subjects) for manuscript evaluation
- **Automated performance visualization** scripts for publication-ready figures

### Documentation

For detailed instructions, see:
- [Testing Guide](tests/README.md) - Complete testing documentation
- [Performance Testing Guide](tests/integration/performance/README.md) - Performance evaluation methodology

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
