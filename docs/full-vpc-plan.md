# Plan: keep all PheBee data paths inside the VPC

Branch: `full_vpc`. Living document; status per item is marked inline.

## Goal

Every component that stores or transports subject, encounter, note or
evidence data must reach its dependencies over private network paths
(VPC endpoints or in-VPC resources), and the HTTP interface must be
reachable only from inside the VPC. Nothing in the repository may name
an account, VPC, endpoint, DNS name or organisation: every site-specific
value is a CloudFormation parameter, and defaults either preserve the
current behaviour or are safe to leave empty.

## Current state (measured on `main`)

| Component | Today | PHI transits? |
|---|---|---|
| 20 of 53 Lambdas | In VPC (Neptune-facing only) | yes |
| 33 of 53 Lambdas | Outside VPC, public AWS endpoints | yes for most |
| EMR Serverless application | No `NetworkConfiguration` | yes (bulk import, materialize, TTL) |
| API Gateway | HTTP API (v2), public, IAM auth | yes (request/response bodies) |
| Neptune | In VPC, IAM auth, encrypted | yes |
| DynamoDB, S3, Athena, Glue | Reached over public service endpoints | yes |
| Step Functions, EventBridge | AWS service integrations, no customer network path | IDs in SFN state only |

Lambda handlers do not inspect `requestContext`, `rawPath` or any other
HTTP-API-specific field. They read `body`, `pathParameters` and
`queryStringParameters` and return explicit `statusCode` dicts, so the
API can change type without touching handler logic (one helper needs a
small fix, see A5).

## Workstreams

### A. API Gateway: HTTP API to REST API with selectable endpoint type

HTTP APIs cannot be private. Only a REST API supports
`EndpointConfiguration.Type: PRIVATE`.

A1. Replace `AWS::Serverless::HttpApi` with `AWS::Serverless::Api` in
    `template.yaml`. New parameters:

    | Parameter | Default | Purpose |
    |---|---|---|
    | `ApiEndpointType` | `REGIONAL` | `REGIONAL` keeps today's public, IAM-signed API. `PRIVATE` makes it reachable only through an execute-api VPC endpoint. |
    | `ExecuteApiVpcEndpointId` | `""` | Required when `ApiEndpointType=PRIVATE` and `CreateVpcEndpoints=false`. |
    | `ApiStageName` | `v1` | REST APIs need a named stage. |

    When PRIVATE: set `EndpointConfiguration.VPCEndpointIds` and a
    resource policy (`Auth.ResourcePolicy.SourceVpcWhitelist`) that
    denies any caller not arriving through that endpoint. A template
    rule asserts that PRIVATE has an endpoint id or endpoint creation on.

A2. Rewrite `api.yaml` for the REST integration format on all 12 routes:
    `type: aws_proxy`, `httpMethod: POST`, same `uri` and `credentials`;
    drop `payloadFormatVersion`, `connectionType`,
    `x-amazon-apigateway-cors` and `x-amazon-apigateway-importexport-version`
    (HTTP-API-only keys). Keep the `sigv4` security scheme; its
    `x-amazon-apigateway-authtype: awsSigv4` form is the REST form.

A3. CORS. The current API allows `*` origins. Callers sign with SigV4,
    which browsers cannot do, so CORS has no legitimate consumer.
    Recommendation: drop it rather than add 12 OPTIONS mock methods.
    **Decision needed.**

A4. Integration timeout. Routes set `timeoutInMillis: 30000`. REST APIs
    cap at 29 000 ms unless the account quota is raised. Set 29 000 and
    document that long `/subjects/query` calls should use direct Lambda
    invocation, which the tests already do. **Decision needed** if you
    would rather request the quota increase.

A5. Payload format 1.0 difference: for a GET with no body, format 2.0
    omits `body`; format 1.0 sends `"body": null`. `extract_body` in
    `layers/phebee-utils/phebee/utils/aws.py` returns `None` in that
    case instead of the event. Treat `None` as absent. Affects
    `GET /source/{source_name}` only.

A6. Outputs. Rename `HttpApiUrl` to `ApiGatewayUrl`, value
    `https://<id>.execute-api.<region>.amazonaws.com/<stage>`. The test
    fixture at `tests/integration/conftest.py:760` already looks for
    `ApiGatewayUrl` first. Add an `ApiId` output so in-VPC callers can
    build the `<id>-<vpce>` hostname when private DNS is off.

A7. Private DNS caveat for the README: enabling private DNS on the
    execute-api endpoint makes every `*.execute-api.<region>` name in
    the VPC resolve privately, which blocks calls from that VPC to any
    public API Gateway. Deployers choose: private DNS on and use the
    plain URL, or private DNS off and use the `<id>-<vpce>` URL.

### B. Lambda functions into the VPC

B1. **Done on `full_vpc`.** Add `VpcConfig` (`SubnetId1`, `SubnetId2`, security groups below)
    to the 32 functions that lack it, using the same explicit block the
    20 in-VPC functions use. Explicit blocks rather than
    `Globals.Function.VpcConfig`, because one function must opt out and
    SAM Globals cannot be unset per resource.

B1a. **Done.** Two security groups, so only Neptune callers get port 8182:

    | Group | Rules | Attached to |
    |---|---|---|
    | `PheBeeLambdaSecurityGroup` (exists) | egress tcp/443 to `ServiceEgressCidr` | every in-VPC function |
    | `PheBeeNeptuneClientSecurityGroup` (new) | egress tcp/8182 to `PheBeeNeptuneSecurityGroup` (standalone egress resource, as today) | the 18 Neptune callers, as a second group |

    `PheBeeNeptuneSecurityGroup` ingress source moves from the Lambda
    group to the client group. Both changes are in-place updates.
    The existing `PheBeeLambdaToNeptuneEgress` resource just changes its
    `GroupId`.

    Neptune callers, traced through `phebee.utils.sparql` and
    `phebee.utils.neptune` rather than taken from today's VpcConfig list:
    CheckBulkLoadStatus, CheckNeptuneBulkLoadStatus, ClearNeptuneGraph,
    CreateEvidence, CreateProject, CreateSubject, ExecuteSparqlReadonly,
    GatherNeptuneBaselineStats, GetLoadStatus, RemoveEvidence,
    RemoveProject, RemoveSubject, ResetDatabase, ResetNeptuneDatabase,
    StartLoad, StartNeptuneBulkLoad, ValidateNeptune,
    ValidateNeptuneComplete.

    Three functions are in the VPC today but never touch Neptune and
    get only the base group: GetSubject, GetSubjectsPhenotypes,
    PrepareEvidencePayload.

B1b. **Done (integration test extended, not yet run against a stack).** Latent bug found by the trace: `RemoveEvidenceFunction` calls
    `delete_term_link` (a Neptune write) when the last evidence for a
    term link is removed, but it is outside the VPC and has no
    `NeptuneEndpoint` environment variable. The call raises, the handler
    logs and swallows it, and the term link is orphaned in the graph.
    Fix in this workstream: VpcConfig with both groups plus the
    `NeptuneEndpoint` and `Region` variables. Add an integration test
    that removes the last evidence and checks the term link is gone.

B1c. External dependency found by the trace: `GetSubjectsPhenotypesFunction`
    calls `https://api-v3.monarchinitiative.org` when a query passes
    `term_association_source_entity`. It is already in the VPC, so this
    path only works where the subnets have NAT or a proxy. It sends a
    disease identifier, never subject data. Options: (a) document that
    the Monarch expansion needs outbound internet and fails with a clear
    error otherwise; (b) preload disease-to-phenotype associations into
    an Iceberg table at ontology-update time, removing the runtime
    dependency. **Decided and done: (a).**
    Connect timeout 5 s; the handler returns HTTP 503 with an explanatory
    message. (b) stays a follow-up ticket.

B2. **Done (`OntologyDownloadInVpc`, default false).** Exception: `DownloadGithubReleaseFunction` fetches ontology
    releases from `api.github.com`. In a VPC without NAT it would fail.
    It handles public ontology files and no subject data.
    Recommendation: keep it outside the VPC and say why in the template
    comment and README. Add parameter `OntologyDownloadInVpc`
    (default `false`) wrapping its `VpcConfig` in `!If`, for sites that
    forbid any out-of-VPC Lambda and have NAT or a proxy.
    **Decision needed.**

B3. **Done; the six explicit roles gained AWSLambdaVPCAccessExecutionRole.** The four CloudFormation custom-resource functions
    (`Create*TableFunction`) answer CloudFormation by PUT to a presigned
    S3 URL. That works through the S3 gateway endpoint. They go in the
    VPC with everything else.

B4. Observability inside the VPC. Lambda log delivery and the
    Lambda-managed X-Ray daemon do not use the function ENI, and
    Powertools metrics travel as EMF log lines, so no logs, xray or
    monitoring endpoint should be needed. Verify during testing (C5).

### C. EMR Serverless into the VPC

C1. Add `NetworkConfiguration` with `SubnetIds` and a new
    `PheBeeEmrSecurityGroup` to `EMRServerlessApplication`.
    Unconditional: the VPC parameters are already mandatory.

C2. EMR jobs need S3 (gateway), Glue and DynamoDB
    (`materialize_all_analytical_tables.py` uses `boto3.resource('dynamodb')`),
    plus STS for the job role. Covered by workstream D.

C3. Subnet sizing note for the README: EMR Serverless workers each take
    an IP; the subnets must have headroom for the configured maximum
    capacity.

### D. VPC endpoints, created optionally

Interface endpoints with private DNS are one-per-service-per-VPC, and
many organisations manage them centrally, so creation must be optional.

D1. Parameters:

    | Parameter | Default | Purpose |
    |---|---|---|
    | `CreateVpcEndpoints` | `false` | Create every endpoint below. |
    | `RouteTableIds` | `""` (CommaDelimitedList) | Required when creating; gateway endpoints attach to route tables. |

D2. Endpoints and why each is needed:

    | Service | Type | Used by |
    |---|---|---|
    | s3 | gateway | all Lambdas, EMR, Neptune bulk loader (already a documented Neptune requirement) |
    | dynamodb | gateway | 21 Lambda call sites, EMR materialize |
    | athena | interface | 16 Lambda call sites |
    | glue | interface | Athena DDL custom resources, EMR Iceberg catalog |
    | events | interface | the four notify functions (`put_events` to `PheBeeBus`) |
    | rds | interface | `neptune.get_cluster_status` (`describe_db_clusters`; Neptune shares the RDS control-plane endpoint) |
    | sts | interface | EMR Serverless job role credentials |
    | execute-api | interface | only when `ApiEndpointType=PRIVATE` |
    | lakeformation | interface | only when `CatalogAccountId` is set |

    Plus a `PheBeeVpcEndpointSecurityGroup` allowing tcp/443 from the
    Lambda and EMR security groups.

D3. When `CreateVpcEndpoints=false`, the README lists the same table as
    a prerequisite checklist and the integration test preflight checks
    for reachability (G3).

### E. Security group clean-up

E1. **Done on `full_vpc` (Lambda SG; EMR SG gets the same rules in C1).**
    `PheBeeLambdaSecurityGroup` egress was `-1 0.0.0.0/0` with a comment
    saying the test suite needs it. The suite invokes Lambdas through the
    Lambda API and API Gateway, neither of which traverses the ENI, so
    that is not the reason. Replace with two rules, applied to the Lambda
    SG and the new EMR SG:

    | Port | Destination | Mechanism |
    |---|---|---|
    | tcp/8182 | `PheBeeNeptuneSecurityGroup` by ID | standalone `AWS::EC2::SecurityGroupEgress` resource, to avoid the circular reference with the Neptune SG's ingress rule |
    | tcp/443 | parameter `ServiceEgressCidr`, default `0.0.0.0/0` | inline rule |

    Sites with centralized interface endpoints in a hub VPC set the
    parameter to their private range (e.g. `10.0.0.0/8`). README caveat:
    gateway endpoints for S3 and DynamoDB route to Amazon's public prefix
    lists, so a restricted CIDR blocks them; such sites need interface
    endpoints for both, or extra egress rules to the managed prefix lists
    (CloudFormation cannot look those IDs up natively, hence the open
    default). Hub-endpoint sites must also associate the hub's private
    hosted zones with the PheBee VPC so service names resolve privately.

E2. **Parameter descriptions done.** Fix the `VpcId` and subnet parameter descriptions that still say the
    VPC is "where Neptune runs". Safe at any time.

    The two `GroupDescription: Amazon Neptune Created` strings are a
    different matter: `GroupDescription` and `GroupName` on
    `AWS::EC2::SecurityGroup` both force replacement. On an existing
    stack the replacement fails on the duplicate `GroupName`, and even
    with a new name the old group's deletion can hit DependencyViolation
    while Lambda ENIs linger. Fix the descriptions only on a fresh
    deployment, or when the stack is being recreated anyway.

### F. Parameters, samconfig, documentation

F1. `samconfig.yaml.example`: add the new parameters to every
    environment, left empty or at defaults.

F2. `README.md`: architecture section (API type, all compute in VPC),
    prerequisites (endpoint checklist, subnet sizing, private DNS
    caveat, where to run the API tests), parameter table, and a short
    "What counts as PHI in PheBee" paragraph so deployers understand
    why. Update `docs/phebee-architecture.drawio` and the PNG.

F3. Version note for `CITATION.cff` / release: this is a breaking
    change for API consumers (URL gains a stage path, CORS removed), so
    it should ship as v1.1.0, not a patch.

### G. Tests and verification

G1. `sam validate --lint` and `cfn-lint` on the template; unit tests
    unchanged.

G2. `extract_body` unit test for the `"body": null` case (A5).

G3. Integration preflight in `tests/integration/conftest.py`: if the API
    is private and the runner cannot connect within a few seconds, skip
    the `tests/integration/api` module with a message rather than
    hanging on the 900 s Lambda timeout config.

G4. Full integration run against a fresh stack deployed with
    `ApiEndpointType=PRIVATE`, `CreateVpcEndpoints=true`, from a runner
    inside the VPC (any EC2 or CodeBuild host in the subnets). Direct
    Lambda invocation tests can still run from a laptop.

G5. Evidence for the compliance team: VPC Flow Logs or endpoint
    CloudWatch metrics showing Lambda and EMR traffic terminating at the
    endpoints, a NAT-free subnet route table, and a failed `curl` to the
    API from outside the VPC.

### H. Out of scope here, worth a follow-up ticket each

- S3 uses SSE-S3 and DynamoDB uses the AWS-owned key. NIST 800-53 SC-28
  reviewers usually ask for a customer-managed KMS key. Parameter-driven,
  separate PR.
- Step Functions execution history and Lambda error logs may contain
  subject and note identifiers. Retention and log-redaction policy.
- Athena query results under `athena-results/<AppName>/` are an
  unmanaged copy of query output. Add a lifecycle rule.
- RDF IRIs hard-code `ods.nationwidechildrens.org` in handlers and in
  the template (`GraphUri` near line 2768). Not a network issue, but it
  is the one organisation-specific string in the repo.

## Decisions needed before coding

1. Drop CORS from the API (A3)? Recommended yes.
2. Cap integration timeout at 29 s, or request the API Gateway quota
   increase (A4)? Recommended cap.
3. Leave the GitHub download function outside the VPC with an opt-in
   parameter (B2)? Recommended yes.
4. Default `ApiEndpointType` to `REGIONAL` so a vanilla deploy still
   works without an execute-api endpoint (A1)? Recommended yes.
5. Create endpoints optionally with default `false` (D1)? Recommended
   yes.
6. Ship as v1.1.0 with a short migration note for API consumers (F3)?

## Suggested commit order on `full_vpc`

1. Security groups, Lambda `VpcConfig` additions, EMR network config,
   parameter and description clean-ups (B, C, E).
2. Optional VPC endpoint resources and parameters (D).
3. REST API conversion, `extract_body` fix, output rename (A).
4. samconfig example, README, architecture diagram, test preflight
   (F, G).
