# xcputils
A library that makes it easy to ingest data from most common sources into most common destinations.

## Sources
- HTTP
- AWS S3
- Azure Data Lake Storage (ADFS)
- Local file
- In-memory string

## Destinations
- AWS S3
- Azure Data Lake Storage (ADFS)
- Local file
- In-memory string

# Examples

## Ingest data from a HTTP endpoint to a string
```
result = xcputils.read_from_http(url="https://postman-echo.com/ip") \
    .write_to_string()
print(result)
```
Output:
```
{
  "ip": "87.59.103.41"
} 
```
## Ingest data from a HTTP endpoint to AWS S3
```
xcputils.read_from_http(url="https://postman-echo.com/ip") \
    .write_to_aws_s3(
        bucket="<enter bucket>",
        file_path="test.json",
        aws_access_key_id="<enter access key id or set env. var. AWS_ACCESS_KEY_ID>",
        aws_secret_access_key="<enter secret access key or set env. var. AWS_SECRET_ACCESS_KEY>"),
        aws_region_name="<enter region or set env. var. AWS_DEFAULT_REGION>",
    )
```
## Ingest data from AWS S3 to a local file
```
xcputils.read_from_aws_s3(
        bucket="<enter bucket>",
        file_path="test.json",
        aws_access_key_id="<enter access key id or set env. var. AWS_ACCESS_KEY_ID>",
        aws_secret_access_key="<enter secret access key or set env. var. AWS_SECRET_ACCESS_KEY>"),
        aws_region_name="<enter region or set env. var. AWS_DEFAULT_REGION>",
    ) \
    .write_to_file("/tmp/test.json")
```
## Ingest data from AWS S3 to Azure Data Lake Storage
```
xcputils.read_from_aws_s3(
    bucket="<enter bucket>",
    file_path="test-folder/test.json",
    aws_access_key_id="<enter access key id or set env. var. AWS_ACCESS_KEY_ID>",
    aws_secret_access_key="<enter secret access key or set env. var. AWS_SECRET_ACCESS_KEY>"),
    aws_region_name="<enter region or set env. var. AWS_DEFAULT_REGION>",
  ) \
  .write_to_adfs(
    container="<enter container>",
    file_name="test.json",
    directory="test-folder",
    storage_account_name="<enter storage account or set env. var. ADFS_DEFAULT_STORAGE_ACCOUNT>",
    tenant_id="<enter tenant id or set env. var. AZURE_TENANT_ID>",
    client_id="<enter client id or set env. var. AZURE_CLIENT_ID>",
    client_secret="<enter client secret or set env. var. AZURE_CLIENT_SECRET>",
  )
```