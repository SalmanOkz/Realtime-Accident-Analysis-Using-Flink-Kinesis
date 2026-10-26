# Project Command Reference

This file records the PowerShell, Terraform, AWS CLI, Python, and Flink SQL commands used to deploy, test, troubleshoot, and remove the real-time accident analysis project.

> Run commands from PowerShell. Replace names, ARNs, account IDs, and paths if you deploy the project in another AWS account or environment.

## 1. Open the project

```powershell
cd "C:\Projects\Data Engineering Projects\Realtime-Accident-Analysis-Using-Flink-Kinesis\Realtime-Accident-Analysis-Using-Flink-Kinesis"
```

Changes the current PowerShell directory to the project root.

```powershell
Get-Location
```

Prints the current working directory so you can confirm where subsequent commands will run.

```powershell
Get-ChildItem -Force
```

Lists normal and hidden files in the current directory.

## 2. Check required software

```powershell
aws --version
```

Shows the installed AWS CLI version and confirms the `aws` command is available.

```powershell
terraform version
```

Shows the installed Terraform version and confirms the `terraform` command is available.

```powershell
python --version
```

Shows the installed Python version.

## 3. Configure and verify AWS access

```powershell
aws configure
```

Stores the AWS access key, secret key, default region, and output format for AWS CLI and SDK use.

```powershell
aws sts get-caller-identity
```

Displays the AWS account ID and principal currently authenticated by the AWS CLI.

```powershell
aws configure get region
```

Prints the default AWS region configured for the current AWS CLI profile.

## 4. Prepare Terraform

The Terraform configuration currently resides under `data\infra`.

```powershell
cd ".\data\infra"
```

Moves from the project root into the Terraform configuration directory.

```powershell
terraform fmt -recursive
```

Formats all Terraform configuration files under the current directory.

```powershell
terraform init
```

Downloads the required Terraform providers and initializes the working directory.

```powershell
terraform validate
```

Checks the Terraform files for syntax errors and internal configuration problems.

```powershell
terraform plan -var-file="environments/dev.tfvars" -out="dev.tfplan"
```

Builds and saves a reviewable deployment plan using the development variables.

```powershell
terraform show "dev.tfplan"
```

Displays all changes contained in the saved Terraform deployment plan.

```powershell
terraform apply "dev.tfplan"
```

Applies exactly the changes stored in `dev.tfplan` and creates the AWS resources.

## 5. Inspect deployed Terraform resources

```powershell
terraform state list
```

Lists every resource currently tracked in the Terraform state.

```powershell
terraform output
```

Displays all Terraform output values, including stream names, bucket name, database, and IAM role ARN.

```powershell
terraform output -raw input_stream_name
```

Prints only the input Kinesis stream name without Terraform formatting.

```powershell
terraform output -raw filtered_stream_name
```

Prints only the filtered Kinesis stream name.

```powershell
terraform output -raw source_bucket_name
```

Prints only the S3 source bucket name.

```powershell
terraform output -raw source_object_key
```

Prints only the key of the sample CSV object stored in S3.

```powershell
terraform output -raw flink_execution_role_arn
```

Prints the ARN of the Terraform-created Flink execution role.

```powershell
terraform output -raw flink_glue_database_name
```

Prints the name of the Glue database created for Flink.

```powershell
terraform output -raw generated_flink_sql_file
```

Prints the path of the generated Flink SQL file.

## 6. Create the Python environment

Run these commands from the project root.

```powershell
python -m venv .venv
```

Creates an isolated Python virtual environment in the `.venv` directory.

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-producer.txt
```

Installs the Python packages required by the Kinesis producer.

## 7. Set producer environment variables manually

```powershell
$env:AWS_REGION = "us-east-1"
```

Sets the AWS region used by the Python producer for the current PowerShell session.

```powershell
$env:ACCIDENT_SOURCE_BUCKET = "accident-analysis-dev-source-<BUCKET_SUFFIX>"
```

Sets the S3 bucket from which the producer downloads the sample accident data.

```powershell
$env:ACCIDENT_SOURCE_KEY = "raw/US_Accidents_Dec20_updated_100samples.csv"
```

Sets the S3 object key of the sample CSV file.

```powershell
$env:KINESIS_INPUT_STREAM = "accident-analysis-dev-input"
```

Sets the Kinesis stream into which the producer publishes accident events.

## 8. Load environment variables from Terraform

Run these commands from `data\infra` while the infrastructure still exists.

```powershell
$env:AWS_REGION = terraform output -raw aws_region
```

Loads the deployed AWS region directly from Terraform output.

```powershell
$env:ACCIDENT_SOURCE_BUCKET = terraform output -raw source_bucket_name
```

Loads the deployed source bucket name directly from Terraform output.

```powershell
$env:ACCIDENT_SOURCE_KEY = terraform output -raw source_object_key
```

Loads the deployed sample-object key directly from Terraform output.

```powershell
$env:KINESIS_INPUT_STREAM = terraform output -raw input_stream_name
```

Loads the deployed input Kinesis stream name directly from Terraform output.

## 9. Verify environment variables

```powershell
Get-ChildItem Env:AWS_REGION
```

Shows the current value of the `AWS_REGION` environment variable.

```powershell
Get-ChildItem Env:ACCIDENT_SOURCE_BUCKET
```

Shows the current source-bucket environment variable.

```powershell
Get-ChildItem Env:ACCIDENT_SOURCE_KEY
```

Shows the current source-object-key environment variable.

```powershell
Get-ChildItem Env:KINESIS_INPUT_STREAM
```

Shows the current input-stream environment variable.

## 10. Run the producer

Run this command from the project root after the Flink `INSERT INTO` job has started.

```powershell
.\.venv\Scripts\python.exe ".\01 - stream-data-app-simulation.py"
```

Reads sample accidents from S3 and publishes them as JSON records to the input Kinesis stream.

## 11. Remove session environment variables

```powershell
Remove-Item Env:AWS_REGION
```

Removes the AWS region variable from the current PowerShell session.

```powershell
Remove-Item Env:ACCIDENT_SOURCE_BUCKET
```

Removes the source-bucket variable from the current PowerShell session.

```powershell
Remove-Item Env:ACCIDENT_SOURCE_KEY
```

Removes the source-object-key variable from the current PowerShell session.

```powershell
Remove-Item Env:KINESIS_INPUT_STREAM
```

Removes the input-stream variable from the current PowerShell session.

## 12. Flink SQL commands

### Create the input table

```sql
%flink.ssql

DROP TABLE IF EXISTS us_accidents_stream;

CREATE TABLE us_accidents_stream (
  `ID` STRING,
  `Severity` BIGINT,
  `Start_Time` TIMESTAMP(3),
  `End_Time` TIMESTAMP(3),
  `City` STRING,
  `County` STRING,
  `State` STRING,
  `Weather_Timestamp` TIMESTAMP(3),
  `Txn_Timestamp` TIMESTAMP(3),
  WATERMARK FOR `Txn_Timestamp` AS `Txn_Timestamp` - INTERVAL '5' SECOND
)
WITH (
  'connector' = 'kinesis',
  'stream' = 'accident-analysis-dev-input',
  'aws.region' = 'us-east-1',
  'scan.stream.initpos' = 'LATEST',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'false'
);
```

Creates a Flink source table that reads newly arriving JSON events from the input Kinesis stream.

### Create the output table

```sql
%flink.ssql

DROP TABLE IF EXISTS us_accidents_filtered;

CREATE TABLE us_accidents_filtered (
  `ID` STRING,
  `Severity` BIGINT,
  `City` STRING,
  `County` STRING,
  `Txn_Timestamp` TIMESTAMP(3)
)
WITH (
  'connector' = 'kinesis',
  'stream' = 'accident-analysis-dev-filtered',
  'aws.region' = 'us-east-1',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);
```

Creates a Flink sink table that writes JSON records to the filtered Kinesis stream.

### Start the streaming filter

```sql
%flink.ssql

INSERT INTO us_accidents_filtered
SELECT `ID`, `Severity`, `City`, `County`, `Txn_Timestamp`
FROM us_accidents_stream
WHERE `Severity` > 3;
```

Starts a continuous job that sends severity 4 and 5 accidents to the filtered stream.

### Create a table for reviewing output

```sql
%flink.ssql

DROP TABLE IF EXISTS accident_results;

CREATE TABLE accident_results (
  `ID` STRING,
  `Severity` BIGINT,
  `City` STRING,
  `County` STRING,
  `Txn_Timestamp` TIMESTAMP(3)
)
WITH (
  'connector' = 'kinesis',
  'stream' = 'accident-analysis-dev-filtered',
  'aws.region' = 'us-east-1',
  'scan.stream.initpos' = 'TRIM_HORIZON',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);
```

Creates a readable Flink table that starts at the oldest retained record in the filtered stream.

### Display filtered records

```sql
%flink.ssql(type=update)

SELECT *
FROM accident_results;
```

Runs a continuous query that displays filtered stream records in Zeppelin.

### Display critical records

```sql
%flink.ssql(type=update)

SELECT *
FROM accident_results
WHERE `Severity` = 5;
```

Displays only critical severity-5 accidents from the filtered stream.

### Count records by severity

```sql
%flink.ssql(type=update)

SELECT `Severity`, COUNT(*) AS accident_count
FROM accident_results
GROUP BY `Severity`;
```

Maintains a continuously updated count of filtered accidents grouped by severity.

## 13. Inspect SNS email subscription

```powershell
aws sns list-subscriptions-by-topic `
  --topic-arn arn:aws:sns:us-east-1:<AWS_ACCOUNT_ID>:accident-analysis-dev-critical-alerts `
  --region us-east-1
```

Shows whether the email endpoint confirmed its subscription to the accident alert topic.

## 14. Inspect Lambda processing

```powershell
aws logs tail "/aws/lambda/accident-analysis-dev-consumer" `
  --since 15m `
  --follow `
  --region us-east-1
```

Continuously prints recent logs from the Lambda that consumes filtered accidents; press `Ctrl+C` to stop.

## 15. Inspect the failure queue

```powershell
$failureQueueUrl = aws sqs get-queue-url `
  --queue-name accident-analysis-dev-consumer-failures `
  --region us-east-1 `
  --query QueueUrl `
  --output text
```

Gets and stores the URL of the Lambda failure queue.

```powershell
aws sqs get-queue-attributes `
  --queue-url $failureQueueUrl `
  --attribute-names ApproximateNumberOfMessages `
  --region us-east-1
```

Shows the approximate number of failed records currently waiting in the SQS queue.

## 16. Inspect the Flink Studio application

```powershell
aws kinesisanalyticsv2 list-applications `
  --region us-east-1 `
  --query "ApplicationSummaries[?ApplicationName=='accident-analysis-dev-studio']" `
  --output json
```

Finds the Studio notebook and displays its current application status.

```powershell
aws kinesisanalyticsv2 describe-application `
  --application-name accident-analysis-dev-studio `
  --region us-east-1
```

Displays the notebook configuration, execution role, timestamps, and current status.

## 17. Stop and delete the manually created Studio notebook

Run these commands before destroying Terraform because the notebook was created outside Terraform.

```powershell
aws kinesisanalyticsv2 stop-application `
  --application-name accident-analysis-dev-studio `
  --region us-east-1
```

Requests that AWS stop the running Flink Studio notebook.

```powershell
aws kinesisanalyticsv2 describe-application `
  --application-name accident-analysis-dev-studio `
  --region us-east-1 `
  --query "ApplicationDetail.ApplicationStatus" `
  --output text
```

Checks whether the notebook has reached the `READY` stopped state.

```powershell
$created = aws kinesisanalyticsv2 describe-application `
  --application-name accident-analysis-dev-studio `
  --region us-east-1 `
  --query "ApplicationDetail.CreateTimestamp" `
  --output text
```

Retrieves the notebook creation timestamp required by the delete API.

```powershell
aws kinesisanalyticsv2 delete-application `
  --application-name accident-analysis-dev-studio `
  --create-timestamp $created `
  --region us-east-1
```

Permanently deletes the stopped Flink Studio notebook.

## 18. Create and apply the Terraform destroy plan

Run these commands from `data\infra`.

```powershell
terraform plan -destroy `
  -var-file="environments/dev.tfvars" `
  -out="destroy.tfplan"
```

Creates a saved plan showing every Terraform-managed resource that will be deleted.

```powershell
terraform show "destroy.tfplan"
```

Displays the destroy plan for review before permanent deletion.

```powershell
terraform apply "destroy.tfplan"
```

Permanently deletes exactly the resources listed in the saved destroy plan.

```powershell
terraform state list
```

Confirms teardown completion when it returns no tracked resources.

## 19. Inspect and remove the Studio-generated IAM role

The AWS Console created this role separately from Terraform, so deleting the notebook did not automatically delete it.

```powershell
aws iam get-role `
  --role-name kinesis-analytics-accident-analysis-dev-studio-us-east-1
```

Checks whether the Studio-generated execution role still exists.

```powershell
aws iam list-attached-role-policies `
  --role-name kinesis-analytics-accident-analysis-dev-studio-us-east-1
```

Lists managed IAM policies attached to the Studio-generated role.

```powershell
aws iam list-role-policies `
  --role-name kinesis-analytics-accident-analysis-dev-studio-us-east-1
```

Lists inline IAM policies stored directly on the Studio-generated role.

```powershell
aws iam delete-role-policy `
  --role-name kinesis-analytics-accident-analysis-dev-studio-us-east-1 `
  --policy-name AccidentAnalysisKinesisAccess
```

Deletes the custom inline Kinesis and Glue permissions from the Studio role.

```powershell
aws iam detach-role-policy `
  --role-name kinesis-analytics-accident-analysis-dev-studio-us-east-1 `
  --policy-arn arn:aws:iam::<AWS_ACCOUNT_ID>:policy/service-role/kinesis-analytics-service-accident-analysis-dev-studio-p-us-east-1
```

Detaches the first AWS Console-generated managed policy from the Studio role.

```powershell
aws iam detach-role-policy `
  --role-name kinesis-analytics-accident-analysis-dev-studio-us-east-1 `
  --policy-arn arn:aws:iam::<AWS_ACCOUNT_ID>:policy/service-role/kinesis-analytics-service-accident-analysis-dev-studio-pi-us-east-1
```

Detaches the second AWS Console-generated managed policy from the Studio role.

```powershell
aws iam delete-policy `
  --policy-arn arn:aws:iam::<AWS_ACCOUNT_ID>:policy/service-role/kinesis-analytics-service-accident-analysis-dev-studio-p-us-east-1
```

Deletes the first generated customer-managed IAM policy after it has been detached.

```powershell
aws iam delete-policy `
  --policy-arn arn:aws:iam::<AWS_ACCOUNT_ID>:policy/service-role/kinesis-analytics-service-accident-analysis-dev-studio-pi-us-east-1
```

Deletes the second generated customer-managed IAM policy after it has been detached.

```powershell
aws iam delete-role `
  --role-name kinesis-analytics-accident-analysis-dev-studio-us-east-1
```

Deletes the empty Studio-generated IAM role.

## 20. Remove the Studio CloudWatch log group

```powershell
aws logs describe-log-groups `
  --region us-east-1 `
  --log-group-name-prefix "/aws/kinesis-analytics/accident-analysis-dev-studio" `
  --query "logGroups[].logGroupName" `
  --output json
```

Finds CloudWatch log groups created by the Studio notebook.

```powershell
aws logs delete-log-group `
  --log-group-name "/aws/kinesis-analytics/accident-analysis-dev-studio" `
  --region us-east-1
```

Permanently deletes the Studio notebook's remaining CloudWatch logs.

## 21. Final cleanup verification

```powershell
aws kinesisanalyticsv2 list-applications `
  --region us-east-1 `
  --query "ApplicationSummaries[?contains(ApplicationName, 'accident-analysis-dev')]" `
  --output json
```

Confirms no matching Flink applications remain when the result is `[]`.

```powershell
aws kinesis list-streams `
  --region us-east-1 `
  --query "StreamNames[?contains(@, 'accident-analysis-dev')]" `
  --output json
```

Confirms no matching Kinesis streams remain when the result is `[]`.

```powershell
aws lambda list-functions `
  --region us-east-1 `
  --query "Functions[?contains(FunctionName, 'accident-analysis-dev')].FunctionName" `
  --output json
```

Confirms no matching Lambda functions remain when the result is `[]`.

```powershell
aws logs describe-log-groups `
  --region us-east-1 `
  --log-group-name-prefix "/aws/kinesis-analytics/accident-analysis-dev-studio" `
  --query "logGroups[].logGroupName" `
  --output json
```

Confirms no matching Studio log groups remain when the result is `[]`.

```powershell
aws resourcegroupstaggingapi get-resources `
  --region us-east-1 `
  --tag-filters Key=Project,Values=accident-analysis Key=Environment,Values=dev `
  --query "ResourceTagMappingList[].ResourceARN" `
  --output json
```

Searches for any remaining AWS resources carrying this project's Terraform tags.

## Recommended command order for another deployment

1. Verify AWS credentials with `aws sts get-caller-identity`.
2. Run `terraform init` and `terraform validate`.
3. Create `dev.tfplan` and review it with `terraform show`.
4. Apply `dev.tfplan`.
5. Create and start the Flink Studio notebook.
6. Start the Flink `INSERT INTO` query.
7. Set the producer environment variables.
8. Run the Python producer.
9. Query the filtered stream and inspect Lambda logs.
10. Stop and delete the manually created notebook.
11. Create and review `destroy.tfplan`.
12. Apply `destroy.tfplan`.
13. Delete manually created Studio IAM and logging resources.
14. Run all final verification commands.
