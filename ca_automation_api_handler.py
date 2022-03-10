import json
import logging
import uuid
import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(event)

    evt_client = boto3.client('events')

    response = evt_client.create_event_bus(
        Name='string',
        EventSourceName='string',
        Tags=[
            {
                'Key': 'string',
                'Value': 'string'
            },
        ]
    )


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
        response_json = json.loads(json.dumps(response_iterator, default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"))

        pretty_channel_json = dict()

        if len(response_json['Items']) > 0:
            for channel_item in response_json['Items']:
                channel_name = channel_item['ChannelName']
                channel_state = channel_item['ChannelState']
                channel_outputs = channel_item['Outputs']
                pretty_channel_json[channel_name] = {"channel_state":channel_state,"outputs":channel_outputs}

        return pretty_channel_json

    try:
        # Path in API call
        path = event['path']

        # Request method
        request_method = event['httpMethod']

        # Body in API Call
        request_body = json.loads(event['body'])

    except Exception as e:
        LOGGER.error("Unable to extract url path, request method, or body from request payload")
        response_json = {"status":"Unable to extract url path, request method, or body from request payload"}
        return api_response(500,response_json)

    # First deal with list uploads. This should be path = /listupload , and method of PUT
    if path == "/listupload":
        if str(request_method) != "PUT":
            response_json = {"status":"For this API resource, we expect HTTP PUT with json payload of the playlist, %s" % (request_method)}
            return api_response(500,response_json)

        LOGGER.info("request body is of type: %s " % (type(request_body)))
        if not isinstance(request_body,dict):
            response_json = {"status":"For this API resource, we expect a json payload"}
            return api_response(500,response_json)


        ## See if we can parse Title from json
        # if we cannot get channel name, create one using a uuid . uuid.uuid4().hex
        try:
            channel_name = request_body['PlayoutChannel']
            if channel_name is None:
                channel_name = ""
        except Exception as e:
            random_uuid = uuid.uuid4().hex
            LOGGER.info("Could not get channel name from playlist, creating new channel: %s " % (random_uuid))
            channel_name = random_uuid

        if len(channel_name) < 1:
            random_uuid = uuid.uuid4().hex
            LOGGER.info("Could not get channel name from playlist, creating new channel: %s " % (random_uuid))
            channel_name = random_uuid


        # Create DB entry for API request
        request_uuid = uuid.uuid4().hex

        create_api_req_record(request_uuid)

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