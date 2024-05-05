import requests
import os
import snowflake.connector as sf
import toml
from dotenv import load_dotenv

# Load configurations
config = toml.load('config.toml')
snowflake_config = config['sf']
os_config = config['os']
url_config = config['url']

def get_file_from_url(url, destination_folder, file_name):
    try:
        response = requests.get(url)
        response.raise_for_status()
        file_path = os.path.join(destination_folder, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print("File obtained successfully from URL")
        return file_path
    except Exception as e:
        print(f"Error getting file from URL: {e}")

def load_file_to_snowflake(file_path, snowflake_config):
    try:
        # Connect to Snowflake
        conn = sf.connect(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account=os.getenv('account'),
            warehouse=snowflake_config['warehouse'],
            database=snowflake_config['database'],
            schema=snowflake_config['schema'],
            role=snowflake_config['role']
        )
        cursor = conn.cursor()
        
        # Use warehouse and schema
        cursor.execute(f"use warehouse {snowflake_config['warehouse']}")
        cursor.execute(f"use schema {snowflake_config['schema']}")
        
        # Truncate table
        cursor.execute(f"truncate table {snowflake_config['schema']}.{snowflake_config['table']}")
        
        # Copy into table
        cursor.execute(f"copy into {snowflake_config['schema']}.{snowflake_config['table']} from @{snowflake_config['stage_name']}/{os.path.basename(file_path)} file_format={snowflake_config['file_format_name']} on_error='continue'")
        
        print("File uploaded successfully into Snowflake")
    except Exception as e:
        print(f"Error loading file to Snowflake: {e}")
    finally:
        # Close connection
        conn.close()

def lambda_handler(event, context):
    try:
        load_dotenv()  # Load environment variables
        file_path = get_file_from_url(url_config['url'], os_config['destination_folder'], os_config['file_name'])
        load_file_to_snowflake(file_path, snowflake_config)
        
        return {
            'statusCode': 200,
            'body': "File obtained from URL and uploaded into Snowflake successfully"
        }
    except Exception as e:
        print(f"Lambda execution failed: {e}")
        return {
            'statusCode': 500,
            'body': "An error occurred during lambda execution"
        }
