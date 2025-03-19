import boto3
import os
import logging
from botocore.config import Config

# Set up basic logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configure the Glue client with shorter timeouts and retry strategy
glue_config = Config(
    connect_timeout=5,  # seconds to wait for a connection
    read_timeout=10,    # seconds to wait for a response
    retries={'max_attempts': 1}  # reduce retries to avoid extra delays
)

def lambda_handler(event, context):
    # Get the Glue job name from environment variables for flexibility
    job_name = os.environ.get('GLUE_JOB_NAME', 'b3scrape_etl')
    
    # Create a Glue client using the custom configuration
    glue_client = boto3.client('glue', config=glue_config)
    
    try:
        logger.info(f"Starting Glue job: {job_name}")
        # Start the Glue job run asynchronously
        response = glue_client.start_job_run(JobName=job_name)
        logger.info(f"Started Glue job {job_name} with response: {response}")
        
        return {
            'statusCode': 200,
            'body': f"Glue job {job_name} started successfully.",
            'jobRunId': response.get('JobRunId')
        }
    except Exception as e:
        logger.error(f"Error starting Glue job: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': f"Error starting Glue job: {str(e)}"
        }
