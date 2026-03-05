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
- Ability to expose data through Lake Formation managed data stores
- Automated deployment and testing workflows

---

## Architecture

PheBee uses a hybrid architecture combining knowledge graphs with data lake technologies to enable both semantic reasoning and analytical queries at scale.

### Core Components

**AWS Neptune (Knowledge Graph)**
- Stores ontology hierarchies (HPO, MONDO, OMIM, Orphanet) as RDF triples
- Enables SPARQL queries for ontological reasoning and relationship traversal

**Apache Iceberg (Data Lake)**
- Stores subject-term associations and clinical evidence as columnar data
- Term-level tables partitioned by:
    - `project_id` and `term_id` for retrieval of matching subjects
    - `subject_id` for subject-level characterization
- Queryable via AWS Athena for analytical workloads

**DynamoDB (Caching Layer)**
- Caches frequently accessed ontology metadata (term descendants, versions)
- Dramatically reduces load for common traversal patterns
- Handles versioning for ontology updates

**S3 (Object Storage)**
- Raw data staging for bulk imports (Phenopackets, NDJSON)
- Iceberg table storage (Parquet files)
- Ontology source files (OWL, OBO)

### Data Flow

1. **Ontology Loading**: OWL/OBO files → Neptune graph + DynamoDB cache
2. **Bulk Import**: S3 NDJSON batches → Step Functions orchestration → Iceberg tables
3. **Query Path**:
   - API Gateway → Lambda → Neptune (ontology traversal) + Athena (data queries)
   - Results combined and returned via RESTful API
4. **Lake Formation Export**: Iceberg tables exposed as managed data stores for external analytics

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
# Using command-line flag
pytest -m integration --existing-stack <your-stack-name> -v

# Or create .phebee-test-stack file for persistent configuration
echo "your-stack-name" > .phebee-test-stack
pytest -m integration -v
```

See [Testing Guide](tests/README.md#using-existing-stack) for details.

Run a specific test:

```bash
pytest tests/integration/test_cloudformation_stack.py::test_cloudformation_stack -m integration -v
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
