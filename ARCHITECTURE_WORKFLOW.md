# Production Architecture Workflow

## 1. Complete system architecture

```mermaid
flowchart TB
    subgraph EXTERNAL[External data providers]
        LIVE[Live traffic API<br/>TomTom / HERE / Open511]
        HIST[NHTSA FARS<br/>historical crash data]
        WEATHER[Weather API<br/>optional enrichment]
    end

    subgraph INGEST[AWS ingestion layer]
        SCHEDULE[EventBridge Scheduler<br/>run every 1-5 minutes]
        SECRET[Secrets Manager<br/>provider API credentials]
        POLLER[Ingestion Lambda<br/>request, validate and normalize]
        CURSOR[(DynamoDB<br/>cursor and deduplication state)]
        RAW[(S3 raw zone<br/>immutable provider responses)]
        INGEST_DLQ[SQS ingestion DLQ]
    end

    subgraph STREAM[AWS streaming layer]
        INPUT[Kinesis input stream<br/>canonical incident events]
        FLINK[Managed Service for Apache Flink<br/>stateful stream processing]
        OUTPUT[Kinesis enriched stream<br/>validated incident changes]
        QUARANTINE[(S3 quarantine zone<br/>invalid or late events)]
    end

    subgraph PROCESS[Operational processing]
        CONSUMER[Consumer Lambda<br/>persist state and evaluate alerts]
        CURRENT[(DynamoDB<br/>current incident state)]
        ALERT_STATE[(DynamoDB<br/>alert idempotency state)]
        EVENT_BUS[EventBridge event bus<br/>alert routing]
        SNS[SNS<br/>email and SMS]
        WEBHOOK[SQS + delivery Lambda<br/>webhooks]
        PROCESS_DLQ[SQS processing DLQ]
    end

    subgraph LAKE[Historical analytics]
        FIREHOSE[Amazon Data Firehose]
        CURATED[(S3 curated zone<br/>partitioned Parquet)]
        GLUE[Glue Data Catalog]
        ATHENA[Athena workgroup]
        BI[Historical dashboard<br/>Grafana or QuickSight]
    end

    subgraph SERVE[Application serving layer]
        API[API Gateway]
        READ[Read API Lambda]
        AUTH[Cognito / OIDC]
        CACHE[Optional CloudFront cache]
        WEB[Web application<br/>live map and incident list]
    end

    subgraph OPS[Operations and governance]
        CW[CloudWatch<br/>logs, metrics and alarms]
        KMS[KMS encryption keys]
        TRAIL[CloudTrail and audit logs]
        BUDGET[AWS Budgets<br/>cost alerts]
    end

    SCHEDULE --> POLLER
    SECRET --> POLLER
    LIVE <--> POLLER
    HIST --> POLLER
    WEATHER --> POLLER
    POLLER --> RAW
    POLLER <--> CURSOR
    POLLER --> INPUT
    POLLER -. failed request or payload .-> INGEST_DLQ

    INPUT --> FLINK
    FLINK --> OUTPUT
    FLINK -. invalid or very late .-> QUARANTINE

    OUTPUT --> CONSUMER
    CONSUMER --> CURRENT
    CONSUMER <--> ALERT_STATE
    CONSUMER --> EVENT_BUS
    CONSUMER -. exhausted processing failure .-> PROCESS_DLQ
    EVENT_BUS --> SNS
    EVENT_BUS --> WEBHOOK

    INPUT --> FIREHOSE
    FIREHOSE --> CURATED
    CURATED --> GLUE
    GLUE --> ATHENA
    ATHENA --> BI

    AUTH --> API
    API --> READ
    READ --> CURRENT
    READ --> ATHENA
    API --> CACHE
    CACHE --> WEB

    CW -. monitors .-> POLLER
    CW -. monitors .-> INPUT
    CW -. monitors .-> FLINK
    CW -. monitors .-> CONSUMER
    CW -. monitors .-> API
    KMS -. encrypts .-> RAW
    KMS -. encrypts .-> INPUT
    KMS -. encrypts .-> CURRENT
    KMS -. encrypts .-> CURATED
    TRAIL -. audits .-> INGEST
    TRAIL -. audits .-> STREAM
    TRAIL -. audits .-> PROCESS
    BUDGET -. cost guardrail .-> OPS
```

## 2. Main live-event workflow

```mermaid
sequenceDiagram
    autonumber
    participant E as EventBridge Scheduler
    participant P as Ingestion Lambda
    participant S as Secrets Manager
    participant T as Traffic API
    participant R as S3 Raw Zone
    participant D as DynamoDB Dedupe
    participant K as Kinesis Input
    participant F as Apache Flink
    participant O as Kinesis Output
    participant C as Consumer Lambda
    participant I as Current Incidents
    participant A as Alert State
    participant N as SNS/Webhook

    E->>P: Start scheduled polling cycle
    P->>S: Read provider credential
    P->>T: Request incidents for configured area
    T-->>P: Return current incident payload
    P->>R: Archive unmodified response

    loop Each returned incident
        P->>P: Validate and normalize schema
        P->>D: Conditional write of provider ID + update time
        alt New or changed incident
            D-->>P: Accepted
            P->>K: Publish canonical event
        else Duplicate incident
            D-->>P: Already processed
            P->>P: Record duplicate metric and skip
        end
    end

    K->>F: Deliver canonical event
    F->>F: Apply event time and watermark
    F->>F: Deduplicate and update lifecycle state
    F->>F: Enrich, score and evaluate rules
    F->>O: Publish validated incident change
    O->>C: Invoke with a record batch
    C->>I: Upsert latest incident state
    C->>A: Conditional alert-id write

    alt New qualifying alert
        A-->>C: Alert accepted
        C->>N: Publish notification event
    else Alert already sent or suppressed
        A-->>C: Duplicate/suppressed
    end
```

## 3. Flink processing workflow

```mermaid
flowchart LR
    SOURCE[Kinesis source] --> DESERIALIZE[Deserialize canonical JSON]
    DESERIALIZE --> VALIDATE{Schema valid?}
    VALIDATE -->|No| BAD[S3 quarantine]
    VALIDATE -->|Yes| TIME[Assign event timestamp<br/>and watermark]
    TIME --> KEY[Key by incident ID]
    KEY --> DEDUPE[Stateful deduplication]
    DEDUPE --> LIFECYCLE[Track created, updated,<br/>cleared lifecycle]
    LIFECYCLE --> ENRICH[Join weather and<br/>road context]
    ENRICH --> SCORE[Calculate transparent<br/>priority score]
    SCORE --> WINDOW[Optional geospatial/time<br/>cluster window]
    WINDOW --> ROUTE{Route result}
    ROUTE -->|Operational change| OUT[Kinesis enriched stream]
    ROUTE -->|Historical aggregate| PARQUET[S3 curated Parquet]
    ROUTE -->|Very late event| LATE[S3 late-event prefix]
```

Flink is justified here because the workflow uses keyed state, event time, deduplication, lifecycle transitions, joins, and windows. If the implementation only filters on severity, replace this layer with Lambda event-source filtering until those stateful requirements exist.

## 4. Incident lifecycle

```mermaid
stateDiagram-v2
    [*] --> New: Provider incident first observed
    New --> Active: Validated and stored
    Active --> Updated: Location, impact, severity or time changes
    Updated --> Updated: Additional provider update
    Updated --> Active: Latest state published
    Active --> Cleared: Provider marks inactive or event disappears after policy threshold
    Updated --> Cleared: Provider marks inactive
    Cleared --> Reopened: Provider reports incident again
    Reopened --> Active: New lifecycle version created
    Cleared --> Archived: Retention period reached
    Archived --> [*]
```

Each transition should retain the source timestamp, ingestion timestamp, previous version, provider, and reason. The current-state table stores the latest version; S3 stores the complete append-only history.

## 5. Alert workflow

```mermaid
flowchart TD
    CHANGE[Incident change] --> RULES[Evaluate severity, priority,<br/>geofence and user rules]
    RULES --> MATCH{Rule matched?}
    MATCH -->|No| END[Store state only]
    MATCH -->|Yes| QUIET{Quiet hours?}
    QUIET -->|Yes| DEFER[Defer or suppress according to rule]
    QUIET -->|No| KEY[Build idempotency key<br/>incident + rule + lifecycle version]
    DEFER --> KEY
    KEY --> WRITE{Conditional DynamoDB write}
    WRITE -->|Already exists| DUP[Do not resend]
    WRITE -->|Created| ROUTER[EventBridge alert router]
    ROUTER --> EMAIL[SNS email/SMS]
    ROUTER --> HOOK[SQS webhook queue]
    HOOK --> DELIVERY[Webhook delivery Lambda]
    DELIVERY --> RESULT{Delivered?}
    RESULT -->|Yes| STATUS[Record delivery status]
    RESULT -->|Retryable failure| RETRY[Backoff and retry]
    RESULT -->|Retries exhausted| DLQ[Webhook DLQ + alarm]
```

## 6. Historical data workflow

```mermaid
flowchart LR
    APIRESP[Raw provider response] --> RAW[S3 raw/provider/year/month/day/hour]
    EVENTS[Canonical stream events] --> FIREHOSE[Firehose buffering]
    FIREHOSE --> JSON[S3 landing JSON]
    JSON --> COMPACT[Scheduled compaction<br/>and data-quality job]
    COMPACT --> PARQUET[S3 curated Parquet<br/>partitioned by event_date/provider]
    PARQUET --> CATALOG[Glue Data Catalog]
    CATALOG --> ATHENA[Athena]
    ATHENA --> DASH[Historical dashboard]
    ATHENA --> EXPORT[Controlled CSV/GeoJSON export]
    RAW --> REPLAY[Replay/reprocessing job]
    REPLAY --> EVENTS
```

The raw zone provides an audit and replay source. Curated Parquet provides efficient analytical queries. Failed validation records belong in a separate quarantine prefix with a failure reason rather than in the curated table.

## 7. API and application workflow

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant Web as Web Map
    participant Auth as Cognito/OIDC
    participant APIGW as API Gateway
    participant Read as Read Lambda
    participant Current as DynamoDB
    participant Athena as Athena

    User->>Web: Open incident dashboard
    Web->>Auth: Authenticate
    Auth-->>Web: Signed token with roles/scopes
    Web->>APIGW: GET /incidents?bbox=...&severity=...
    APIGW->>APIGW: Authorize, throttle and validate
    APIGW->>Read: Validated request
    Read->>Current: Query current incidents/geographic index
    Current-->>Read: Active incidents
    Read-->>Web: Paginated GeoJSON/JSON response

    User->>Web: Request historical trend
    Web->>APIGW: GET /analytics/trends?from=...&to=...
    APIGW->>Read: Authorized analytics request
    Read->>Athena: Run bounded/predefined query
    Athena-->>Read: Aggregated result
    Read-->>Web: Trend data and freshness metadata
```

Do not allow arbitrary public Athena SQL. Use bounded, parameterized query templates, enforce date/region limits, cache common results, and expose source/ingestion freshness to users.

## 8. Failure and recovery workflow

```mermaid
flowchart TD
    FAILURE{Failure location}
    FAILURE -->|Provider request| APIRETRY[Exponential backoff + jitter<br/>honor Retry-After]
    APIRETRY --> APILIMIT{Attempts exhausted?}
    APILIMIT -->|Yes| IDLQ[Ingestion DLQ + alarm]
    APILIMIT -->|No| POLL[Retry provider request]

    FAILURE -->|Invalid payload| QUAR[S3 quarantine<br/>payload + reason]
    QUAR --> DATAALARM[Data-quality alarm]

    FAILURE -->|Kinesis consumer record| PARTIAL[Return partial batch failure]
    PARTIAL --> RETRY[Retry from failed sequence]
    RETRY --> LIMIT{Retry/age limit reached?}
    LIMIT -->|Yes| PDLQ[Failure destination + alarm]
    LIMIT -->|No| PARTIAL

    FAILURE -->|Flink task/job| CHECKPOINT[Restart from checkpoint]
    CHECKPOINT --> HEALTH{Checkpoint restores?}
    HEALTH -->|No| SNAPSHOT[Restore tested snapshot/savepoint]
    HEALTH -->|Yes| CATCHUP[Process backlog and monitor lag]

    FAILURE -->|Bad deployment| ROLLBACK[Deploy previous immutable artifact]
    ROLLBACK --> SNAPSHOT
```

Operational runbooks should define who owns each alarm, how to inspect the failed record, when to replay it, and how to avoid duplicate alerts during recovery.

## 9. Terraform infrastructure workflow

```mermaid
flowchart LR
    subgraph SOURCE[Version control]
        CODE[Application code]
        TF[Terraform modules]
        SCHEMA[Event schemas]
        TESTS[Tests and fixtures]
    end

    PR[Pull request] --> CHECK[Format, validate, lint,<br/>unit and contract tests]
    CODE --> PR
    TF --> PR
    SCHEMA --> PR
    TESTS --> PR
    CHECK --> BUILD[Build immutable Lambda ZIPs<br/>and Flink artifact]
    BUILD --> SCAN[Dependency, secret and<br/>IaC security scans]
    SCAN --> PLAN[Terraform plan for dev]
    PLAN --> REVIEW[Plan review]
    REVIEW --> APPLY[OIDC CI role applies dev]
    APPLY --> INTEGRATION[Integration and replay tests]
    INTEGRATION --> LOAD[Load and failure tests in staging]
    LOAD --> APPROVAL[Production approval]
    APPROVAL --> SNAP[Create Flink snapshot/savepoint]
    SNAP --> PROD[Apply production plan]
    PROD --> VERIFY[Verify health, freshness,<br/>lag and checkpoints]
    VERIFY -->|Healthy| DONE[Release complete]
    VERIFY -->|Unhealthy| ROLLBACK[Rollback artifact/config<br/>and restore state]
```

### Terraform module dependency map

```mermaid
flowchart TD
    BOOT[bootstrap<br/>remote state + CI role] --> FOUNDATION[foundation<br/>KMS + shared tags]
    FOUNDATION --> ING[ingestion<br/>schedule + Lambda + secret + DLQ]
    FOUNDATION --> STR[streaming<br/>Kinesis input/output]
    FOUNDATION --> DATA[data_lake<br/>S3 + Firehose + Glue + Athena]
    STR --> FLINK[flink<br/>application + role + logs]
    ING --> STR
    STR --> DATA
    FLINK --> CONSUME[consumers<br/>Lambda + DynamoDB + alert bus]
    CONSUME --> SERVING[serving<br/>API Gateway + auth + read API]
    DATA --> SERVING
    ING --> OBS[observability<br/>dashboards + alarms]
    STR --> OBS
    FLINK --> OBS
    CONSUME --> OBS
    SERVING --> OBS
```

## 10. Repository structure

```text
.
├── app/
│   ├── ingestion/              # Live API adapter and canonical mapper
│   ├── flink/                  # Stateful processing application
│   ├── incident_consumer/      # Current state and alert decisions
│   ├── webhook_delivery/       # Retried external notifications
│   └── read_api/               # Dashboard/API queries
├── schemas/
│   ├── canonical-incident-v1.json
│   └── provider-mappings/
├── infra/
│   ├── bootstrap/
│   ├── modules/
│   │   ├── foundation/
│   │   ├── ingestion/
│   │   ├── streaming/
│   │   ├── flink/
│   │   ├── data_lake/
│   │   ├── consumers/
│   │   ├── serving/
│   │   └── observability/
│   └── environments/
│       ├── dev/
│       ├── staging/
│       └── prod/
├── tests/
│   ├── unit/
│   ├── contract/fixtures/
│   ├── integration/
│   ├── replay/
│   └── load/
├── dashboards/
├── runbooks/
└── .github/workflows/
```

## 11. Build order

Build this architecture as six independently testable milestones:

1. **Foundation:** canonical schema, provider adapter, tests, configuration, and structured logging.
2. **Terraform ingestion:** remote state, IAM, KMS, Secrets Manager, scheduled Lambda, S3 raw storage, DynamoDB dedupe, Kinesis, and DLQ.
3. **Stream processing:** Flink dedupe/lifecycle logic, checkpoints, stable operator IDs, output stream, quarantine, and replay tests.
4. **Operational consumer:** current-state DynamoDB table, idempotent alert state, SNS/webhook routing, partial batch failures, and alarms.
5. **Analytics and UI:** Firehose, curated Parquet, Glue/Athena, secured read API, live map, freshness indicators, and historical charts.
6. **Hardening:** load testing, recovery exercises, security scanning, cost budgets, runbooks, staging promotion, and rollback validation.

Each milestone should produce observable evidence before the next begins. For example, milestone 2 is complete only when one provider fixture is archived, deduplicated, normalized, and visible in Kinesis with alarms configured.

## 12. Lean portfolio variant

If cost is the main constraint, begin with this architecture:

```mermaid
flowchart LR
    EB[EventBridge] --> ING[Ingestion Lambda]
    API[Traffic API] <--> ING
    ING --> K[Kinesis on-demand]
    ING --> S3[(S3 raw)]
    K --> PROC[Processing Lambda]
    PROC --> DB[(DynamoDB incidents)]
    PROC --> SNS[SNS alerts]
    K --> FH[Firehose]
    FH --> LAKE[(S3 Parquet/Athena)]
    DB --> READ[API Gateway + read Lambda]
    READ --> UI[Live map]
```

This version still demonstrates event-driven ingestion, streaming, idempotency, a data lake, Infrastructure as Code, and a user-facing product. Add Flink after implementing a requirement that needs stateful event-time processing.

## 13. Recommended implementation choice

For this portfolio project:

- use Open511 or 511 SF Bay for the first live feed;
- deploy the lean variant first with Terraform;
- preserve the Kinesis interface so Flink can be introduced without changing ingestion;
- implement incident lifecycle and five-minute geographic clustering in Flink;
- use DynamoDB for current state and S3/Parquet/Athena for history;
- build a small map that shows event freshness and filter controls; and
- publish measured latency, throughput, replay correctness, and monthly cost in the README.

This produces a complete, defensible project while keeping the first deployment affordable.
