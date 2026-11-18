# PheBee networking

PheBee is bring-your-own-VPC. Every component that stores or moves subject,
encounter or note identifiers runs inside the VPC you supply: all Lambda
functions, the EMR Serverless application that runs bulk imports and
rebuilds, and the Neptune cluster. The stack never creates VPC endpoints,
NAT gateways or routes, and never changes DNS in your VPC. This document
explains what the subnets must provide, which parameters fit which
situation, and how the optional private API works.

## What the subnets must provide

Lambda functions in a VPC have no public address, so the subnets need a path
to every AWS service PheBee calls:

| Service | Used by |
|---|---|
| S3 | all functions, EMR, Neptune bulk loads |
| DynamoDB | all functions, EMR |
| Athena | query and materialization functions |
| Glue | Athena DDL, EMR Iceberg catalog |
| EventBridge | the notify functions |
| STS | EMR Serverless job credentials |
| RDS control plane (`rds.<region>.amazonaws.com`) | Neptune cluster status |
| Lake Formation | only when `CatalogAccountId` is set |
| execute-api | only for a PRIVATE API, used by API clients, not by PheBee |

Either a NAT gateway or VPC endpoints satisfy this. Two features need the
public internet and degrade clearly without it:

- **Ontology updates** download releases from GitHub. The downloader runs
  outside the VPC by default (`OntologyDownloadInVpc=false`), so it works
  without NAT.
- **Monarch expansion** (`term_association_source_entity` on the subjects
  query) calls `api-v3.monarchinitiative.org` from inside the VPC. Without
  an egress path the query returns HTTP 503 with an explanatory message.

## Choosing parameters

| Situation | What to set |
|---|---|
| **Private subnets behind a NAT gateway.** The simplest and cheapest setup; the VPC console's "VPC and more" wizard builds it in a few clicks. | Nothing. All defaults apply: public API with IAM authentication, open egress. |
| **An organisation that provides endpoints centrally**, for example from a hub VPC, and forbids internet egress. | `ServiceEgressCidr` narrowed to the private range that reaches the hub (for example `10.0.0.0/8`). The hub's hosted zones must be associated with the PheBee VPC so service names resolve privately. S3 and DynamoDB must be reached through **interface** endpoints: gateway endpoints route to Amazon's public prefix lists, which a narrowed CIDR blocks. |
| **Private subnets, no NAT, no central networking team.** | Create interface endpoints (with private DNS) for Athena, Glue, EventBridge, STS and RDS yourself, plus gateway endpoints for S3 and DynamoDB on the subnets' route tables, then deploy with defaults. Interface endpoints are billed hourly, roughly USD 7 per endpoint per availability zone per month. |

A default VPC with public subnets and no NAT does not work for PheBee and
never has: functions in a VPC never get a public address.

### Network parameters

| Parameter | Default | Purpose |
|---|---|---|
| `VpcId`, `SubnetId1`, `SubnetId2` | required | The VPC and two private subnets in different availability zones |
| `SubnetId3` | empty | Optional third subnet in a third zone |
| `ServiceEgressCidr` | `0.0.0.0/0` | Destination for tcp/443 egress from Lambda, EMR and Neptune security groups |
| `OntologyDownloadInVpc` | `false` | Put the GitHub downloader in the VPC (needs NAT or a proxy to GitHub) |
| `ApiEndpointType` | `REGIONAL` | `PRIVATE` for a VPC-endpoint-only API |
| `ExecuteApiVpcEndpointId` | empty | Existing execute-api endpoint, required for a PRIVATE API |
| `AssociateApiVpcEndpoint` | `true` | Set `false` when that endpoint belongs to another account |
| `ApiStageName` | `v1` | First path segment of the API URL |

## Security groups

| Group | Rules | Attached to |
|---|---|---|
| Lambda | out: tcp/443 to `ServiceEgressCidr` | every function |
| Neptune client | out: tcp/8182 to the Neptune group | only the functions that call Neptune |
| EMR | out: tcp/443 to `ServiceEgressCidr` | the EMR Serverless application |
| Neptune | in: tcp/8182 from the Neptune client group; out: tcp/443 to `ServiceEgressCidr` (S3 bulk loads) | the cluster |

Nothing else in the VPC can reach port 8182.

## Private API

By default the API is a REST API on a public hostname that accepts only
SigV4-signed requests from IAM principals in the deploying account. With
`ApiEndpointType=PRIVATE` the API has no public hostname at all: from
outside the VPC its name does not resolve, and API Gateway admits only
requests arriving through the endpoint named in `ExecuteApiVpcEndpointId`.
Request and response bodies therefore never leave the VPC.

Switching an existing API between REGIONAL and PRIVATE updates it in place;
the API ID and URL do not change.

### How clients reach it

An execute-api interface endpoint is a property of the VPC, not of the API;
one endpoint serves any number of private APIs. Clients inside the network
have three ways to address the API:

1. **Default hostname**, `https://<api-id>.execute-api.<region>.amazonaws.com/<stage>`
   (the `ApiGatewayUrl` output). Works when the endpoint has private DNS
   enabled, or when your networking team publishes a hosted zone for API
   Gateway names into your VPC.
2. **Endpoint-specific hostname**, `https://<api-id>-<vpce-id>.execute-api.<region>.amazonaws.com/<stage>`
   (the `ApiGatewayVpceUrl` output). Needs no DNS configuration, but exists
   only when the endpoint is associated with the API (`AssociateApiVpcEndpoint=true`),
   which API Gateway can do only for an endpoint in the same account.
3. **A custom domain** of your own pointing at the endpoint, with your
   certificate. Independent of the other two.

Without any of these, a client can connect to the endpoint's own DNS name
and send the default hostname in the `Host` header.

### Caveats

- **Private DNS on an execute-api endpoint applies to every API Gateway
  hostname in that VPC.** Public API Gateway APIs called from inside the
  VPC then fail unless they use a custom domain. Use the endpoint-specific
  hostname or a custom domain if that matters.
- **Shared endpoints owned by another account** cannot be associated with
  the API; set `AssociateApiVpcEndpoint=false`. The resource policy still
  restricts access to that endpoint.
- The endpoint's security group must allow tcp/443 from wherever your API
  clients run.

## Verifying a deployment

From inside the VPC (an EC2 instance, or a CloudShell VPC environment),
with AWS credentials:

```bash
eval "$(aws configure export-credentials --format env)"
curl -sS --aws-sigv4 "aws:amz:<region>:execute-api" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
  -H "x-amz-security-token: $AWS_SESSION_TOKEN" \
  https://<api-id>.execute-api.<region>.amazonaws.com/v1/source/hpo
```

A 200 with source metadata, or a 404 saying no source is found, proves the
path and the signature. From outside the VPC the same hostname should fail
to resolve.

The integration suite's API tests probe the API once and skip, with a
message, when the runner cannot reach a private API; run them from inside
the VPC to exercise that path.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| A Lambda times out after 90 s on its first AWS call | The subnets have no path to that service: missing endpoint or NAT route, or a service name resolving to a public address while `ServiceEgressCidr` is narrowed |
| Neptune bulk load fails to read S3 | S3 reached through a gateway endpoint while egress is narrowed to a private range |
| Private API returns `{"message":"Forbidden"}` | The request reached API Gateway through an endpoint other than `ExecuteApiVpcEndpointId` |
| Private API hostname does not resolve inside the VPC | The endpoint has private DNS off and no hosted zone is published; use `ApiGatewayVpceUrl` or a custom domain |
| Stack fails creating the API with an endpoint error | The endpoint belongs to another account; set `AssociateApiVpcEndpoint=false` |
| Monarch queries return 503 | Expected where the subnets have no internet egress |
