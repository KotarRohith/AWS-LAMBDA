
```markdown
**Lambda Function: AirBnb Data Loader**

**Overview**

This AWS Lambda function is designed to fetch data from an S3 bucket, perform data cleaning and conversion, and load it into a PostgreSQL database. The function runs on a daily schedule at 9:00 AM using a cron job.

## Prerequisites

Before deploying and running the Lambda function, make sure you have the following:

- AWS account with appropriate IAM roles and permissions.
- Secrets Manager configured with AWS credentials and database credentials.
- S3 bucket containing the AirBnb data file (ABNB_NYC_2019.csv).
- PostgreSQL database with the necessary schema.

## Configuration

Ensure the following configurations are set:

### AWS Credentials

- AWS access key and secret key are stored in AWS Secrets Manager.
- Secrets Manager ARN for AWS credentials: `arn:aws:secretsmanager:us-east-1:637423517599:secret:aws_secret-OlDeax`

### Database Credentials

- PostgreSQL database credentials are stored in AWS Secrets Manager.
- Secrets Manager ARN for database credentials: `arn:aws:secretsmanager:us-east-1:637423517599:secret:postgres_secret-uP50lV`

### S3 Bucket

- S3 bucket name: `airbnb-nyc`
- Raw data key: `ABNB_NYC_2019.csv`

### PostgreSQL Database

- Database name: `postgres`
- Target table name: `warehouse_table2`

## Table Schema

The target PostgreSQL table has the following schema:

```sql
CREATE TABLE IF NOT EXISTS warehouse_table2 (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    host_id INT,
    host_name VARCHAR(255),
    neighbourhood_group VARCHAR(255),
    neighbourhood VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    room_type VARCHAR(255),
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT,
    last_updated_date DATE
);
```

## Execution

The Lambda function executes the following steps:

1. Retrieves AWS credentials from Secrets Manager.
2. Retrieves database credentials from Secrets Manager.
3. Connects to the S3 bucket and fetches the raw data file (ABNB_NYC_2019.csv).
4. Cleans and converts data types in the DataFrame.
5. Replaces NaN values in the DataFrame.
6. Transforms and loads data into the PostgreSQL database using batch processing.

## Deployment

- Deploy the Lambda function in the AWS Management Console.
- Set up a daily cron job to trigger the Lambda function at 9:00 AM.

## Troubleshooting

If the Lambda function encounters issues, check the CloudWatch Logs for detailed error messages.
```

## Output

