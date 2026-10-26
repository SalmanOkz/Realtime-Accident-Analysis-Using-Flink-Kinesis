# Installation and Run Guide — Stage 1

This guide runs the first version of the project on Windows:

```text
Sample CSV in S3
    -> Python replay producer
    -> Kinesis input stream
    -> Flink Studio SQL filter (Severity > 3)
    -> Kinesis filtered stream
    -> Lambda consumer
    -> CloudWatch metric + SNS severity-5 alert
```

The instructions assume PowerShell, one AWS account, and `us-east-1` for every service.

## 1. Understand what will and will not be automated

Terraform creates:

- the S3 source bucket and uploads the sample CSV;
- the input and filtered Kinesis streams;
- the Lambda consumer and event-source mapping;
- the SNS topic and optional email subscription;
- the SQS failed-record destination;
- the Glue database;
- IAM roles and policies;
- CloudWatch log retention and alarms; and
- an environment-specific `infra/generated-flink.sql` file.

You must perform these steps manually:

1. authenticate the AWS CLI;
2. enter an optional email address in `dev.tfvars`;
3. create and start the Flink Studio notebook in the AWS Console;
4. paste and run the generated Flink SQL;
5. run the Python producer; and
6. stop/delete the manually created notebook before destroying Terraform resources.

The Studio notebook is manual because the repository contains an exported Zeppelin note rather than a compiled, deployable Flink application artifact.

## 2. Expected cost

This project creates billable AWS resources. Managed Service for Apache Flink is likely to be the largest cost because the notebook consumes capacity while it is running. Kinesis, Lambda, CloudWatch, SNS, SQS, S3, and Glue may also incur charges.

Stop the Studio notebook whenever you are not using it. Destroy the development infrastructure after completing the exercise. Check current pricing for your selected Region before deployment.

## 3. Install the local tools

### 3.1 Open a new PowerShell window

Use a normal PowerShell window for current-user installations. Administrator access is only needed for system-wide installations.

### 3.2 Verify Git and Python

Run:

```powershell
git --version
python --version
```

Python is already present in the current development environment. Python 3.11 or newer is suitable for the local producer.

If Git is missing:

```powershell
winget install --id Git.Git --exact
```

If Python is missing:

```powershell
winget install --id Python.Python.3.13 --exact
```

Close and reopen PowerShell after installation if a command is not found.

### 3.3 Install Terraform

Using Windows Package Manager:

```powershell
winget install --id Hashicorp.Terraform --exact
```

Then open a new PowerShell window and verify:

```powershell
terraform version
```

Terraform must be version 1.6 or newer. If the package is unavailable, download the Windows AMD64 ZIP from the official [Terraform installation page](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli), extract `terraform.exe`, and add its directory to the user `PATH`.

### 3.4 Install AWS CLI v2

AWS currently documents this current-user PowerShell installer:

```powershell
irm https://awscli.amazonaws.com/v2/install.ps1 | iex
```

Open a new PowerShell window and verify:

```powershell
aws --version
```

Use AWS CLI v2. The official alternatives, including MSI installation, are documented in the [AWS CLI installation guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

### 3.5 Verify everything together

```powershell
git --version
python --version
terraform version
aws --version
```

All four commands must succeed before continuing.

## 4. Prepare the AWS account

### 4.1 Choose one Region

Use `us-east-1` throughout the first exercise. Terraform already defaults to this Region. Do not create the Flink notebook in another Region.

### 4.2 Do not use root credentials

Use an IAM Identity Center/SSO profile or a dedicated IAM principal. Do not create access keys for the AWS account root user.

For a learning account, the deploying identity needs permission to create and manage resources in:

- S3;
- Kinesis Data Streams;
- Lambda;
- IAM roles and policies, including `iam:PassRole`;
- SNS and SQS;
- CloudWatch Logs, metrics, and alarms;
- Glue Data Catalog;
- Managed Service for Apache Flink; and
- AWS-managed KMS key aliases used by Kinesis.

An administrator can be used temporarily in a dedicated learning account. In a shared or company account, request an approved sandbox role instead.

### 4.3 Authenticate with IAM Identity Center/SSO

If your account uses AWS IAM Identity Center:

```powershell
aws configure sso --profile accident-dev
aws sso login --profile accident-dev
$env:AWS_PROFILE = "accident-dev"
$env:AWS_REGION = "us-east-1"
```

Follow the browser login and select the correct account and permission set. AWS documents this flow in its [CLI SSO guide](https://docs.aws.amazon.com/en_en/cli/latest/userguide/cli-configure-sso-tutorial.html).

### 4.4 Alternative: configure an access-key profile

Only use this if SSO is unavailable and the keys belong to a dedicated IAM user:

```powershell
aws configure --profile accident-dev
$env:AWS_PROFILE = "accident-dev"
$env:AWS_REGION = "us-east-1"
```

The wizard asks for:

```text
AWS Access Key ID
AWS Secret Access Key
Default region: us-east-1
Default output: json
```

Never put these credentials in Terraform files, `.tfvars`, Python files, or Git.

### 4.5 Confirm the active identity

```powershell
aws sts get-caller-identity
```

Check the returned account number and ARN carefully. Terraform will deploy into that account.

## 5. Configure the project

From the repository root:

```powershell
Set-Location 'C:\Projects\Data Engineering Projects\Realtime-Accident-Analysis-Using-Flink-Kinesis\Realtime-Accident-Analysis-Using-Flink-Kinesis'
Copy-Item '.\infra\environments\dev.tfvars.example' '.\infra\environments\dev.tfvars'
notepad '.\infra\environments\dev.tfvars'
```

The initial file contains:

```hcl
aws_region               = "us-east-1"
project_name             = "accident-analysis"
environment              = "dev"
alert_email              = null
alert_severity_threshold = 5
log_retention_days       = 14
```

### Values you may change

- Set `alert_email` to your email address if you want email notifications:

  ```hcl
  alert_email = "your-address@example.com"
  ```

- Keep `alert_email = null` if you do not want email.
- Keep `aws_region = "us-east-1"` for the first run.
- Keep the project name and environment unchanged until the first deployment works.

### Files you should not edit for the first run

- Do not manually change stream names in Python.
- Do not manually change bucket names.
- Do not edit resource ARNs.
- Do not edit `infra/generated-flink.sql`; Terraform generates it.
- Do not use the old hard-coded names from the exported `.zpln` notebook.

## 6. Initialize and validate Terraform

Move into the Terraform directory:

```powershell
Set-Location '.\infra'
terraform init
terraform fmt -recursive
terraform validate
```

Expected result:

```text
Success! The configuration is valid.
```

`terraform init` downloads the AWS, Archive, Local, and Random providers. Internet access is required.

If `terraform validate` fails, stop here and resolve that error before deploying.

## 7. Review and apply the infrastructure

Create a saved plan:

```powershell
terraform plan -var-file='.\environments\dev.tfvars' -out='dev.tfplan'
```

Review the plan. It should create, among other supporting resources:

- one S3 bucket and one uploaded CSV object;
- two Kinesis streams;
- one Lambda function;
- one Lambda event-source mapping;
- one SNS topic;
- one SQS queue;
- one Glue database;
- IAM roles/policies; and
- CloudWatch log and alarm resources.

Apply the exact saved plan:

```powershell
terraform apply 'dev.tfplan'
```

Do not close the terminal while Terraform is applying.

After a successful apply:

```powershell
terraform output
```

Keep the `infra/terraform.tfstate` file. It records which AWS resources Terraform manages. It is ignored by Git and must not be committed because state can contain sensitive information.

## 8. Confirm the SNS email subscription

If you supplied `alert_email`, open the AWS SNS confirmation message and select **Confirm subscription**. Alerts are not delivered until confirmation. Also check the spam folder.

The same topic currently carries accident alerts and infrastructure alarms, so the address may receive both types during this learning stage.

## 9. Create the Flink Studio notebook manually

AWS documents Studio notebooks as interactive SQL/Python/Scala environments backed by a Glue database. See the official [Managed Service for Apache Flink Studio tutorial](https://docs.aws.amazon.com/managed-flink/latest/java/example-notebook.html).

### 9.1 Print the Terraform-created values

From `infra`:

```powershell
terraform output -raw input_stream_name
terraform output -raw filtered_stream_name
terraform output -raw flink_execution_role_arn
terraform output -raw flink_glue_database_name
terraform output -raw generated_flink_sql_file
```

### 9.2 Open the correct AWS Console Region

1. Sign in to the AWS Console.
2. Select **US East (N. Virginia) / us-east-1**.
3. Search for **Managed Service for Apache Flink**.
4. Open **Studio notebooks**.
5. Choose **Create Studio notebook**.

### 9.3 Configure the notebook

Use:

```text
Notebook name: accident-analysis-dev-studio
AWS Glue database: value from flink_glue_database_name
Service execution role: existing role from flink_execution_role_arn
Runtime: latest supported Studio runtime
Networking: no custom VPC required for this Kinesis-only learning pipeline
```

If the console offers to create a new IAM role, choose the existing Terraform-created role instead. The identity using the console needs permission to pass that role.

Create the notebook, choose **Run**, and wait until its status becomes **Running**. AWS recommends using the latest supported Studio runtime.

### 9.4 Open Apache Zeppelin

1. Choose the running notebook.
2. Select **Open in Apache Zeppelin**.
3. Create a new note named `accident-analysis-stage-1`.
4. Add a paragraph whose first line is:

   ```text
   %flink.ssql(type=update)
   ```

5. Open the Terraform-generated SQL locally:

   ```powershell
   notepad '.\generated-flink.sql'
   ```

6. Paste the SQL under the `%flink.ssql(type=update)` line.
7. Run the paragraph.

The SQL creates source and sink tables and starts a continuous insert for records where `Severity > 3`.

Run this Flink job before starting the producer because the source begins at `LATEST`.

## 10. Prepare the Python producer

Open a second PowerShell window and return to the repository root:

```powershell
Set-Location 'C:\Projects\Data Engineering Projects\Realtime-Accident-Analysis-Using-Flink-Kinesis\Realtime-Accident-Analysis-Using-Flink-Kinesis'
python -m venv '.venv'
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r '.\requirements-producer.txt'
```

Using `.venv\Scripts\python.exe` directly avoids PowerShell activation-policy problems.

## 11. Give the producer AWS permissions

Print the Terraform-created policy ARN:

```powershell
Set-Location '.\infra'
terraform output -raw producer_policy_arn
```

If your current profile already has administrator access in a learning account, it can run the producer without another attachment.

For a restricted identity, an IAM administrator must attach the printed managed policy to the user/role or incorporate its permissions into the SSO permission set. The policy only permits reading the Terraform-uploaded CSV, writing the input stream, and using the Kinesis encryption key.

## 12. Set producer configuration automatically

From the `infra` directory:

```powershell
$env:AWS_REGION = terraform output -raw aws_region
$env:ACCIDENT_SOURCE_BUCKET = terraform output -raw source_bucket_name
$env:ACCIDENT_SOURCE_KEY = terraform output -raw source_object_key
$env:KINESIS_INPUT_STREAM = terraform output -raw input_stream_name
```

If you authenticated with a named profile, also set:

```powershell
$env:AWS_PROFILE = "accident-dev"
```

Verify non-secret configuration:

```powershell
$env:AWS_REGION
$env:ACCIDENT_SOURCE_BUCKET
$env:ACCIDENT_SOURCE_KEY
$env:KINESIS_INPUT_STREAM
aws sts get-caller-identity
```

Do not print access keys or secret keys.

## 13. Run the event simulator

Return to the repository root and run:

```powershell
Set-Location '..'
.\.venv\Scripts\python.exe '.\01 - stream-data-app-simulation.py'
```

The local sample contains 99 data rows. The producer should print one JSON status line per record and finish with something similar to:

```json
{"replay":1,"sent":99,"failed":0}
```

It waits 0.25 seconds between records. To run more quickly during a repeat test:

```powershell
.\.venv\Scripts\python.exe '.\01 - stream-data-app-simulation.py' --delay-seconds 0
```

Do not use `--repeat 3` unless you intentionally want duplicate demo events.

## 14. Verify the complete pipeline

### 14.1 Verify Flink

In Zeppelin or the Flink dashboard:

- the continuous SQL job should remain running;
- input records should increase;
- output records should include only severity 4 and 5; and
- there should be no parse or permission failures.

The sample should produce 20 filtered events: 17 severity-4 records and 3 severity-5 records.

### 14.2 Verify Lambda logs

In AWS Console:

```text
CloudWatch -> Log groups -> /aws/lambda/accident-analysis-dev-consumer
```

Search for `Processed accident`. There should be log entries for filtered events.

Alternatively:

```powershell
aws logs tail '/aws/lambda/accident-analysis-dev-consumer' --follow --region us-east-1
```

Press `Ctrl+C` to stop following logs.

### 14.3 Verify custom metrics

Open:

```text
CloudWatch -> Metrics -> All metrics -> Custom namespaces
-> RealtimeAccidentAnalysis/dev -> FilteredAccidentCount
```

The dimension combinations are severity, city, and county.

### 14.4 Verify alerts

With the default threshold of 5, the three severity-5 sample records should trigger SNS publications. A confirmed email subscription should receive messages.

This Stage 1 consumer is at-least-once. Retried Kinesis records can duplicate metrics or notifications. Idempotent alerting is a later improvement.

### 14.5 Verify the failed-record queue

In the SQS console, open the queue whose name ends in `consumer-failures`. Its visible message count should be zero.

## 15. Common failures

### `terraform` or `aws` is not recognized

Close and reopen PowerShell so that its `PATH` is refreshed. Verify the installation again.

### `terraform init` cannot download providers

Check internet access, proxy configuration, firewall rules, and access to `registry.terraform.io`.

### `AccessDenied` during Terraform apply

Read the denied action and resource in the error. The deploying identity may lack a required service permission or `iam:PassRole`. Confirm the active account with `aws sts get-caller-identity` before changing permissions.

### S3 bucket name already exists

The configuration adds a random suffix, so this should be rare. If Terraform state was deleted after creating the bucket, recover/import the state or use a different project name. Do not manually create another bucket with the same Terraform-managed name.

### SNS emails do not arrive

Confirm the subscription from the original SNS email and check the spam folder. In the SNS Console, the subscription must show `Confirmed`, not `PendingConfirmation`.

### Producer reports expired credentials

For SSO:

```powershell
aws sso login --profile accident-dev
$env:AWS_PROFILE = "accident-dev"
```

Then retry.

### Producer gets `AccessDenied` on S3 or Kinesis

Attach the `producer_policy_arn` policy to the active principal or use an approved role containing equivalent permissions. Confirm that the producer and resources are all in `us-east-1`.

### Flink receives no records

Check these in order:

1. the notebook is `Running`;
2. the streaming SQL paragraph is still running;
3. the SQL contains the Terraform-created input stream name;
4. the notebook and stream are both in `us-east-1`;
5. the producer ran after the Flink job started because the connector uses `LATEST`; and
6. the Flink role can read the input stream and use its KMS key.

If you ran the producer before Flink, run it again after the Flink job is active.

### Lambda receives no events

Check whether the filtered stream contains output records. If it does not, inspect the Flink job first. If it does, check that the Lambda event-source mapping is enabled and inspect `IteratorAge`, `Errors`, and Lambda logs.

### Flink JSON timestamp parsing fails

Use the updated producer in this repository. It emits ISO-8601 timestamps compatible with the generated SQL. Do not use the original hard-coded producer from an older checkout.

### Terraform cannot delete an IAM role

Stop and delete the manually created Flink Studio notebook first. It may still reference the Terraform-managed execution role.

## 16. Stop the environment safely

### Temporary pause

When you plan to continue later:

1. stop the Flink Studio notebook;
2. leave Terraform resources in place; and
3. remember that some resources may still incur charges.

### Complete cleanup

First stop and delete `accident-analysis-dev-studio` from the Managed Service for Apache Flink console.

Then run from `infra`:

```powershell
terraform plan -destroy -var-file='.\environments\dev.tfvars' -out='destroy.tfplan'
terraform apply 'destroy.tfplan'
```

Confirm:

```powershell
terraform show
```

Terraform should report no managed resources after a successful destroy.

The development S3 bucket uses `force_destroy = true`; its sample object and versions are permanently deleted during destroy.

## 17. Successful completion checklist

- [ ] `terraform validate` succeeds.
- [ ] `terraform apply` succeeds.
- [ ] The SNS email subscription is confirmed, if configured.
- [ ] The Flink Studio notebook is running in `us-east-1`.
- [ ] The generated SQL job is running before the producer starts.
- [ ] The producer reports `sent=99` and `failed=0`.
- [ ] Flink outputs 20 severity-4/5 events.
- [ ] Lambda logs show processed accidents.
- [ ] CloudWatch contains `FilteredAccidentCount` metrics.
- [ ] Severity-5 alerts are delivered.
- [ ] The SQS failed-record queue remains empty.
- [ ] The Studio notebook is stopped when testing ends.
