import json
import logging
import uuid
import boto3
import base64
import datetime
import os

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(event)

    # boto3 client initialization
    emt_client = boto3.client('mediatailor')
    evt_client = boto3.client('events')
    db_client = boto3.client('dynamodb')

    # initialize exceptions list to capture issues and exit if necessary
    exceptions = []
    exceptions.clear()

    ## Functions Start

    # DynamoDB Put Item // Create record of list translation
    def create_api_req_record(request_uuid,request_body_b64,channel_name):
        # fields for Db record: request_id (primary key), list_translation, channel_creation, channel_programs, vod_sources, clip_transcodes, clip_packaging, list (b64 encoded)

        api_request_database = os.environ['APIREQDB']

        db_item = {
            "request_id": request_uuid,
            "list_b64":request_body_b64,
            "channel_name":channel_name,
            "status":{
                "channel_creation":"not_started",
                "channel_programs":"not_started",
                "vod_sources":"not_started",
                "transcodes":"not_started",
                "packaging":"not_started"
            }
        }

        # DYNAMO DB JSON BUILDER
        dynamodb_item = dict()
        keylist = []
        def dict_path(dicttopopulate,my_dict):
            for k,v in my_dict.items():

                if isinstance(v,dict):
                    dynamodb_item_subdict = dict()
                    dict_path(dynamodb_item_subdict,v)

                    v = dynamodb_item_subdict
                    dicttopopulate.update({k:{"M":v}})

                elif isinstance(v,str):
                    dicttopopulate.update({k:{"S":v}})
                elif isinstance(v,list):
                    for i in range(0,len(v)):
                        dynamodb_item_list = dict()
                        dict_path(dynamodb_item_list,v[i])

                        v[i] = {"M":dynamodb_item_list}

                    dicttopopulate.update({k:{"L":v}})

        dict_path(dynamodb_item,db_item)

        try:
            response = db_client.put_item(TableName=api_request_database,Item=dynamodb_item)
        except Exception as e:
            exceptions.append("Unable to create/update item in DynamoDB, got exception:  %s" % (str(e).upper()))
            return exceptions
        return response


    # DynamoDB Get Item // record of list translation
    def get_api_req_record(request_uuid):

        api_request_database = os.environ['APIREQDB']

        try:
            response = db_client.get_item(TableName=api_request_database,Key={"request_id":{"S":request_uuid}})
        except Exception as e:
            exceptions.append("Unable to get item from database, got exception:  %s" % (str(e).upper()))
            return exceptions
        return response

    # API Response template
    def api_response(response_code,response_body):
        return {
            'statusCode': response_code,
            'body': json.dumps(response_body)
        }


    def initialize_step_functions(request_uuid):
        event_detail = {
            "request":request_uuid,
            "list":json.loads(event['body']),
            "clips":{},
            "workflow_status":{}
        }

        try:
            event_publish_response = evt_client.put_events(
                Entries=[
                    {
                        "Source": "lambda.amazonaws.com",
                        "DetailType": "StepFunctions Initialize",
                        "Detail": json.dumps(event_detail)
                    },
                ]
            )
        except Exception as e:
            event_publish_response = ""
            LOGGER.error("Unable to send EventBridge event to start list ingest and translation. Please try again later: %s " % (e))
            exceptions.append("Unable to send EventBridge event to start list ingest and translation. Please try again later: %s " % (e))
        return event_publish_response

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

        try:
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
        except Exception as e:
            LOGGER.error("Unable to get channel list from MediaTailor: %s " % (e))
            exceptions.append("Unable to get channel list from MediaTailor: %s " % (e))

    ## Functions End


    try:
        # Path in API call
        path = event['path']

        # Request method
        request_method = event['httpMethod']

    except Exception as e:
        LOGGER.error("Unable to extract url path, request method, or body from request payload")
        response_json = {"status":"Unable to extract url path, request method, or body from request payload"}
        return api_response(500,response_json)

    # First deal with list uploads. This should be path = /listupload , and method of PUT
    if path == "/listupload":
        if str(request_method) != "PUT":
            response_json = {"status":"For this API resource, we expect HTTP PUT with json payload of the playlist, %s" % (request_method)}
            return api_response(500,response_json)

        try:
            # Body in API Call
            request_body = json.loads(event['body'])
            request_body_b64 = base64.b64encode(event['body'].encode("utf-8"))
        except Exception as e:
            LOGGER.error("Unable to extract body from request payload")
            response_json = {"status":"Unable to extract url path, request method, or body from request payload"}
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
        create_api_req_record(request_uuid,request_body_b64, channel_name)

        if len(exceptions) > 0:
            response_json = {"status":"Unable to process, try again later","exceptions":exceptions}
            return api_response(500,response_json)

        # Send custom EventBridge to start Step workflow
        # send this json structure:
        #   List: {request_body}
        #   workflow_status: {tx,emp,list}
        #   API UUID: {random uuid to track this request}
        # Note the time of execution, log response
        initialize_step_functions(request_uuid)

        if len(exceptions) > 0:
            response_json = {"status":"Unable to process, try again later","exceptions":exceptions}
            return api_response(500,response_json)

        current_time = datetime.datetime.utcnow().isoformat() + 'Z'
        url_for_updates = "https://%s/%s/liststatus/%s" % (event['requestContext']['domainName'],event['requestContext']['stage'],request_uuid)

        response_json = {
            "Playout Channel Name": channel_name,
            "Request ID":request_uuid,
            "URL for Translation Updates": url_for_updates,
            "Initiation Time": current_time
        }
        return api_response(200,response_json)

    elif "/liststatus" in path:

        request_uuid = path.split("/")[-1]

        # lookup request_id in API Req DB
        request_status = get_api_req_record(request_uuid)

        if len(exceptions) > 0:
            response_json = {"status":"Unable to get status update from database","exceptions":exceptions}
            return api_response(500,response_json)

        # manipulate response and return to sender
        # placeholder ## do this later

        response_json = {"status":request_status}
        return api_response(200,response_json)

    elif path == "/channels":

        channel_list = mediatailor_get_channels()

        if len(exceptions) > 0:
            response_json = {"status":"Unable to get channel list from MediaTailor","exceptions":exceptions}
            return api_response(500,response_json)

        return api_response(200,channel_list)

    else:
        response_json = {"status":"You are here because your URL is malformed or incorrect. Please refer to the CloudFormation Stack Outputs to determine correct API call syntax"}
        return api_response(500,response_json)