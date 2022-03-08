import json
import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(event)
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }
