import pandas as pd
import boto3
import psycopg2
import json

bucket_name = 'airbnb-nyc'
raw_data_key = 'ABNB_NYC_2019.csv'

def get_aws_credentials():
    aws_secret_name = "arn:aws:secretsmanager:us-east-1:637423517599:secret:aws_secret-OlDeax"
    try:
        client = boto3.client('secretsmanager', region_name='us-east-1')
        response = client.get_secret_value(SecretId=aws_secret_name)
        secret_string = response['SecretString']
        secret_dict = json.loads(secret_string)
        print("successful")
        return secret_dict
    except Exception as e:
        print(f"Error retrieving AWS credentials: {e}")
        return None

def get_db_credentials():
    db_secret_name = "arn:aws:secretsmanager:us-east-1:637423517599:secret:postgres_secret-uP50lV"
    try:
        client = boto3.client('secretsmanager', region_name='us-east-1')
        response = client.get_secret_value(SecretId=db_secret_name)
        secret_string = response['SecretString']
        secret_dict = json.loads(secret_string)
        return secret_dict
    except Exception as e:
        print(f"Error retrieving database credentials: {e}")
        return None
        
db_credentials = get_db_credentials()
if db_credentials:

    connection_params = {
        "user": db_credentials.get('username', ''),
        "password": db_credentials.get('password', ''),
        "host": db_credentials.get('host', ''),
        "port": "5432",
        "database": "postgres"
    }

# checking  table if not exists
def create_table_if_not_exists(cursor, table_name, table_schema):
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
    table_exists = cursor.fetchone()[0]
    if not table_exists:
        cursor.execute(table_schema)

# Defining the table schema
table_schema = """
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
"""

#  cleaning and converting data types
def clean_and_convert_data_types(df):
    for column in df.columns:
        try:
            df[column] = pd.to_numeric(df[column], errors='raise').astype('Int64')
        except (ValueError, TypeError):
            try:
                df[column] = pd.to_numeric(df[column], errors='raise')
            except (ValueError, TypeError):
                try:
                    df[column] = pd.to_datetime(df[column], errors='raise')
                except (ValueError, TypeError):
                    pass
    return df

#replacing NaN values
def replace_nan_values(df):
    print("in replace")
    for column in df.columns:
        if df[column].dtype == 'O':  
            df[column] = df[column].fillna('UNKNOWN')
        elif pd.api.types.is_numeric_dtype(df[column].dtype):
            df[column] = df[column].fillna(0)
        elif pd.api.types.is_datetime64_dtype(df[column].dtype):  
            mode_value = df[column].mode().iloc[0] 
            df[column] = pd.to_datetime(df[column], errors='coerce').fillna(mode_value)
    return df

#transforming and loading data into PostgreSQL with batch processing
def transform_and_load_to_postgres_batch(cleaned_data, connection_params, table_name, table_schema, batch_size=1000):
    connection = psycopg2.connect(**connection_params)
    cursor = connection.cursor()

    # Creating the table if it doesn't exist
    create_table_if_not_exists(cursor, table_name, table_schema)

    # Batch processing
    for start_index in range(0, len(cleaned_data), batch_size):
        end_index = start_index + batch_size
        batch_data = cleaned_data.iloc[start_index:end_index]

        for index, row in batch_data.iterrows():
            id, price, minimum_nights, last_review, reviews_per_month = row['id'], row['price'], row['minimum_nights'], row['last_review'], row['reviews_per_month']

            cursor.execute("SELECT * FROM warehouse_table2 WHERE id = %s", (id,))
            existing_record = cursor.fetchone()

            if existing_record:
                if (
                    existing_record[9] != price or
                    existing_record[10] != minimum_nights or
                    existing_record[11] != last_review or
                    existing_record[12] != reviews_per_month
                ):
                    cursor.execute("""
                        UPDATE warehouse_table2
                        SET
                            price = %s,
                            minimum_nights = %s,
                            last_review = %s,
                            reviews_per_month = %s,
                            last_updated_date = CURRENT_DATE
                        WHERE
                            id = %s
                    """, (price, minimum_nights, last_review, reviews_per_month, id))
            else:
                cursor.execute("""
                    INSERT INTO warehouse_table2 (
                        id, name, host_id, host_name, neighbourhood_group, neighbourhood,
                        latitude, longitude, room_type, price, minimum_nights, number_of_reviews,
                        last_review, reviews_per_month, calculated_host_listings_count,
                        availability_365, last_updated_date
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
                """, (
                    id, row['name'], row['host_id'], row['host_name'],
                    row['neighbourhood_group'], row['neighbourhood'], row['latitude'],
                    row['longitude'], row['room_type'], price, minimum_nights,
                    row['number_of_reviews'], last_review, reviews_per_month,
                    row['calculated_host_listings_count'], row['availability_365']
                ))

    connection.commit()

    cursor.close()
    connection.close()

def lambda_handler(event, context):
    # Retrieving AWS credentials
    aws_credentials = get_aws_credentials()
    if aws_credentials:
        aws_access_key_id = aws_credentials.get('accessKeyId', '')
        aws_secret_access_key = aws_credentials.get('secretAccessKey', '')
        region = aws_credentials.get('region', 'us-east-1')

        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region)
        obj = s3.get_object(Bucket=bucket_name, Key=raw_data_key)
        raw_data = pd.read_csv(obj['Body'])

        cleaned_data = clean_and_convert_data_types(raw_data)

        cleaned_data = replace_nan_values(cleaned_data)
        transform_and_load_to_postgres_batch(cleaned_data, connection_params, 'warehouse_table2', table_schema, batch_size=1000)
        
        return {
            'statusCode': 200,
            'body': 'Data loaded successfully!'
        }
    else:
        return {
            'statusCode': 500,
            'body': 'Error retrieving AWS credentials!'
        }
