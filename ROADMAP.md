# PheBee Project Roadmap

PheBee  is an evolving platform designed to enable rapid, flexible querying of phenotype and disease data at scale using modern cloud-native infrastructure and knowledge graph technologies.

This document outlines our planned milestones and the focus areas for each version.

---

## Version 0.1 — Initial Open Source Release

**Goal:** Establish the core AWS infrastructure and provide basic support for subject-term associations. Migrate from codebase tailored for internal use to one appropriate for general use.

### Features

- REST API to support:
  - Adding and querying phenotype and disease terms for subjects
  - Linking terms using ontology relationships
- Lambda functions to support:
  - Adding and querying phenotype and disease terms for subjects
  - Management of projects
  - Import and export of Phenopacket data
  - Retrieval of cohorts based on shared/similar phenotypes
- Ontology support for:
  - Human Phenotype Ontology (HPO)
  - Mondo Disease Ontology
- Core AWS services established:
  - Ontology loaders with support for automatic updates
  - Neptune graph database
  - Step Functions and Lambda for orchestration
  - DynamoDB for term source metadata
  - EventBridge for pipeline interaction
- Integration testing and validation framework
  - Lambda coverage
  - API coverage
- IAM roles and permissions scoped for production use

---

## Version 0.9.1 — Data Model Refinement and Provenance Support

**Released:** July 18, 2025

### Features

- Enhanced subject-term links with hash-based IRIs:
  - Deterministic termlink identification to avoid duplicates
  - Evidence-driven termlink creation and management
  - Creator tracking (human vs automated, with creator IDs)
  - Timestamp recording (when terms were added)
- Bulk import support:
  - Step Functions orchestration for large-scale data ingestion
  - Phenopacket format processing
  - Validation and error handling workflows
- ECO ontology integration:
  - Evidence and Conclusion Ontology support
  - Evidence assertion type classification
  - Automated ECO ontology updates
- Subject query improvements:
  - Optimized SPARQL query performance
  - Better handling of creator fields

---

## Version 1.0.0-rc1 — Iceberg Data Lake Integration (Release Candidate)

**Released:** March 2, 2026

**Goal:** Production-ready system with analytical query capabilities and proven performance at scale

### Features

- Iceberg data lake architecture:
  - Dual-partitioned analytical tables (`subject_terms_by_subject`, `subject_terms_by_project_term`)
  - AWS Glue Data Catalog registration
  - Athena-queryable evidence and ontology hierarchy tables
  - Incremental subject-level materialization during bulk import
- DynamoDB caching layer:
  - Term descendant caching for faster hierarchy traversal
  - Ontology version caching
  - Dramatically reduced Neptune load for common patterns
- Performance testing infrastructure:
  - Comprehensive performance evaluation suite
  - Realistic synthetic data generation with disease clustering
  - Reproducible benchmark datasets (1K-100K subjects)
  - API latency and bulk import throughput testing
- Evidence query enhancements:
  - Evidence-driven termlink management (removed direct termlink API endpoints)
  - Query evidence by various filters and provenance metadata
- Monarch Knowledge Graph integration:
  - Query subjects using Monarch disease-phenotype associations
  - Expanded term discovery capabilities
- System improvements:
  - Cascading deletes for projects and subjects
  - Integration test suite refactoring
  - EMR Serverless 7.10.0 for Spark-based processing

---

## Version 1.0.0 — Production Release (Planned)

**Goal:** Validated production deployment at hospital scale

### Planned Features

- Automated clinical data feed integration
- Security and compliance hardening for production clinical data
- Advanced Lake Formation permissions and external catalog federation
- Comprehensive validation at hospital scale

---

# Future Improvements

### Planned Features

- Add built-in Sagemaker notebook deployment to CloudFormation stack
- Evaluate additional automated ontology integrations
- Evaluate production usage patterns and consider further caching to speed up commonly-used paths
- Allow dynamic semantic queries through API

---

## Notes

This roadmap is a living document. Features and priorities may shift based on community feedback and internal requirements.

