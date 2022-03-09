import json
import logging
import uuid
import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(event)


    # boto3 client initialization
    emt_client = boto3.client('mediatailor')

    # API Response template
    def api_response(response_code,response_body):
        return {
            'statusCode': response_code,
            'body': json.dumps(response_body)
        }

    # DB call to see if channel exists
    # def dbGetSingleChannelInfo(channeldb,channel):
    #     LOGGER.debug("Doing a call to Dynamo to get channel information for channel : %s" % (channel))
    #     try:
    #         response = db_client.get_item(TableName=channeldb,Key={"channelid":{"S":channel}})
    #     except Exception as e:
    #         exceptions.append("Unable to get item from DynamoDB, got exception:  %s" % (str(e).upper()))
    #         return exceptions
    #     return response

    # Get list of channels from MediaTailor Channel Assembly
    def mediatailor_get_channels():

        # initialize paginator
        paginator = emt_client.get_paginator('list_channels')

        response_iterator = paginator.paginate().build_full_result()

        return response_iterator

    return str(mediatailor_get_channels())

    try:
        # Path in API call
        path = event['path']

        # Request method
        request_method = event['httpMethod']

        # Body in API Call
        request_body = event['body']
    except Exception as e:
        LOGGER.error("Unable to extract url path, request method, or body from request payload")
        response_json = {"status":"Unable to extract url path, request method, or body from request payload"}
        return api_response(500,response_json)

    # First deal with list uploads. This should be path = /listupload , and method of PUT
    if path == "/listupload":
        if str(request_method) != "PUT":
            response_json = {"status":"For this API resource, we expect HTTP PUT with json payload of the playlist, %s" % (request_method)}
            return api_response(500,response_json)

        if request_body is None or isinstance(request_body,dict) != False:
            response_json = {"status":"For this API resource, we expect a json payload"}
            return api_response(500,response_json)


        ## See if we can parse Title from json
        # uuid.uuid4().hex
        try:
            channel_name = request_body['PlayoutChannel']
        except Exception as e:
            random_uuid = uuid.uuid4().hex
            LOGGER.info("Could not get channel name from playlist, creating new channel: %s " % (random_uuid))
            channel_name = random_uuid

        if len(channel_name) < 1 or channel_name is None:
            random_uuid = uuid.uuid4().hex
            LOGGER.info("Could not get channel name from playlist, creating new channel: %s " % (random_uuid))
            channel_name = random_uuid

        # Create DB entry for channel
        # Check if channel exists, if not, create


        # Create DB entry for API request

        # Send custom EventBridge to start Step workflow
        # send this json structure:
        #   List: {request_body}
        #   workflow_status: {tx,emp,list}
        #   API UUID: {random uuid to track this request}
        # Note the time of execution, log response

        # Return to sender with:
        # Playout channel name
        # Request ID , random UUID that they can use in API call to get status updates on this list translation (UUID)
        # Status : In progress (for example)
        # Time started
        # API URL to use to get updates


    response_json = {"status":"For this API resource, we expect a json payload"}
    return api_response(500,response_json)