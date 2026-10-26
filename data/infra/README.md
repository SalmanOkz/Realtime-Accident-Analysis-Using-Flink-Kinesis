# Complete Terraform deployment

This configuration implements the complete target architecture:

```text
Terraform-uploaded sample CSV (S3 raw layer)
  -> local Python/Boto3 replay producer
  -> Kinesis input stream
  -> Flink SQL in a Studio notebook
  -> Kinesis enriched stream
       |-> Lambda -> CloudWatch metrics/logs + SNS alerts + SQS failure destination
       `-> Data Firehose -> Parquet in S3 curated layer
                              -> Glue Data Catalog
                              -> Athena workgroup and named queries
                              -> optional Amazon Managed Grafana workspace
```

Firehose reads the **enriched stream produced by Flink**, so Athena and Grafana analyze the same filtered records consumed by Lambda.

## What Terraform manages

- Raw/source and analytics S3 buckets with encryption, versioning, public-access blocking, and TLS-only policies
- Input and enriched Kinesis Data Streams
- IAM roles and least-privilege policies for Flink, Firehose, Lambda, and the producer
- Data Firehose delivery with JSON-to-Snappy-Parquet conversion
- Glue databases plus JSON conversion and curated Parquet table definitions
- Athena workgroup, encrypted result location, and example named queries
- Lambda consumer, SNS email alerts, SQS failure destination, CloudWatch logs, alarms, and dashboard
- Optional Amazon Managed Grafana workspace configured for Athena and CloudWatch
- Environment-specific Flink SQL generated as `generated-flink.sql`

## Manual boundary

The repository contains an exported Zeppelin notebook rather than a compiled Flink application artifact. Create the Managed Service for Apache Flink Studio notebook manually, select the Terraform-created role and Glue database, and run `generated-flink.sql`. Terraform manages its supporting resources.

Grafana user access and dashboards are completed after workspace creation because user IDs come from IAM Identity Center and dashboard design is an application concern.

## Prerequisites

- Terraform 1.6 or newer
- AWS CLI v2
- Python 3.11 or newer
- AWS credentials authorized to create the resources in this directory
- Access to Managed Service for Apache Flink Studio
- IAM Identity Center enabled in `aws_region` when `enable_grafana = true`

Do not put AWS access keys in `.tfvars` files or source control.

## Configure

```powershell
Copy-Item environments/dev.tfvars.example environments/dev.tfvars
```

Edit `environments/dev.tfvars`:

```hcl
alert_email = "you@example.com"

# Keep false until IAM Identity Center is enabled in this Region.
enable_grafana = false

# When Grafana is enabled, optionally provide Identity Center user IDs.
grafana_admin_user_ids = []
```

## Validate and deploy

```powershell
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -var-file=environments/dev.tfvars -out=dev.tfplan
terraform show dev.tfplan
terraform apply dev.tfplan
terraform output
```

Review the plan carefully because Firehose, Athena, S3, CloudWatch, Flink Studio, and Grafana can generate usage charges.

## Configure Flink Studio

1. Create a Studio notebook in the same Region.
2. Select `flink_execution_role_arn` from Terraform outputs.
3. Select `flink_glue_database_name` from Terraform outputs.
4. Start the notebook and open Zeppelin.
5. Run the table-creation statements from `generated-flink.sql`.
6. Start its continuous `INSERT INTO` statement.
7. Leave the job running while replaying records.

The Flink job reads the input stream, keeps records where `Severity > 3`, and writes them into the enriched stream. Both Lambda and Firehose consume that enriched stream independently.

## Run the producer

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-producer.txt
```

From this Terraform directory, print the exact environment-variable commands:

```powershell
terraform output -raw producer_powershell
```

Set those commands in the active PowerShell session, return to the repository root, and run:

```powershell
.\.venv\Scripts\python.exe '.\01 - stream-data-app-simulation.py'
```

## Verify real-time processing

```powershell
aws logs tail "/aws/lambda/accident-analysis-dev-consumer" --since 15m --follow --region us-east-1
```

Confirm:

- severity 4 and 5 events reach Lambda;
- severity 5 events publish SNS alerts;
- the SQS failure queue remains empty;
- the CloudWatch dashboard displays Kinesis, Lambda, and Firehose metrics.

## Verify curated Parquet delivery

Firehose buffers records before writing, so the first object is not immediate:

```powershell
$analyticsBucket = terraform output -raw analytics_bucket_name
aws s3 ls "s3://$analyticsBucket/curated/accidents/" --recursive
```

Conversion failures are stored under `s3://<analytics-bucket>/errors/firehose/`.

Inspect Firehose delivery logs:

```powershell
$firehoseName = terraform output -raw firehose_delivery_stream_name
aws logs tail "/aws/kinesisfirehose/$firehoseName" --since 30m --region us-east-1
```

## Query with Athena

Open Athena, select the workgroup printed by `athena_workgroup_name`, and run:

```sql
SELECT *
FROM accident_analysis_dev_analytics.accidents_curated
LIMIT 20;
```

Example severity aggregation:

```sql
SELECT city, severity, count(*) AS accident_count
FROM accident_analysis_dev_analytics.accidents_curated
GROUP BY city, severity
ORDER BY accident_count DESC
LIMIT 100;
```

The table uses partition projection for `year`, `month`, `day`, and `hour`; it does not require a crawler or `MSCK REPAIR TABLE` after every delivery.

## Enable Grafana

1. Enable IAM Identity Center in the deployment Region.
2. Set `enable_grafana = true`.
3. Optionally add Identity Center user IDs to `grafana_admin_user_ids`.
4. Generate and review a new Terraform plan.
5. Apply the reviewed plan.
6. Open `grafana_workspace_endpoint` from Terraform outputs.
7. Configure panels against the Athena workgroup and CloudWatch metrics.

Grafana is optional by default because workspace creation fails when IAM Identity Center is unavailable and a running workspace incurs charges.

## Destroy

Stop and delete the manually created Flink Studio notebook first. Then run:

```powershell
terraform plan -destroy -var-file=environments/dev.tfvars -out=destroy.tfplan
terraform show destroy.tfplan
terraform apply destroy.tfplan
```

Development buckets use `force_destroy = true`, so applying this destroy plan removes their objects and versions. A manually created Grafana dashboard or Studio-generated role may require separate cleanup if it was created outside Terraform.

