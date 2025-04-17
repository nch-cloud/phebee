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

## Version 0.2 — Data Model Refinement and Provenance Support (In Progress)

**Goal:** Introduce richer provenance tracking and expand the data model to better support real-world usage scenarios. Continue migration of internal provenance code to open source release.

### Planned Features

- Revise RDF model to include additional context and provenance information
- Enhanced subject-term links:
  - Track how each term was added (e.g., manual entry, automated pipeline)
  - Record who added the term and when
  - Capture contextual metadata (e.g., clinical note reference, span bounds)
- Additional clinical information capture
  - Allow terms at project/encounter/note level context
  - Capture information on patient encounters (dates, id)
  - Capture information on clinical notes (author, provider type, etc.)
- API improvements
  - Add phenopacket support to API layer
  - Improved querying on provenance and other metadata
- Additional test suite coverage

---

## Notes

This roadmap is a living document. Features and priorities may shift based on community feedback and internal requirements.

