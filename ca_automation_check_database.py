import json
import logging
import uuid
import boto3
import base64
import datetime
import os

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
content_table_name = os.environ['CONTENTDB']

def lambda_handler(event, context):
    LOGGER.info(event)

    # boto3 client initialization
    db_client = boto3.client('dynamodb')

    # initialize exceptions list to capture issues and exit if necessary
    exceptions = []
    exceptions.clear()

    ## Functions Start


    # JSON_TO_DYNAMODB_BUILDER
    def json_to_dynamo(dicttopopulate,my_dict):
        for k,v in my_dict.items():

            if isinstance(v,dict):

                dynamodb_item_subdict = dict()
                json_to_dynamo(dynamodb_item_subdict,v)


                v = dynamodb_item_subdict
                dicttopopulate.update({k:{"M":v}})

            elif isinstance(v,str):
                dicttopopulate.update({k:{"S":v}})
            elif isinstance(v,int):
                dicttopopulate.update({k:{"S":str(v)}})
            elif isinstance(v,list):

                new_item_list = []
                for i in range(0,len(v)):
                    dynamodb_item_list = dict()
                    json_to_dynamo(dynamodb_item_list,v[i])

                    #v[i] = {"M":dynamodb_item_list}
                    new_item_list.append({"M":dynamodb_item_list})

                dicttopopulate.update({k:{"L":new_item_list}})

    # DYNAMODB_JSON_DECONSTRUCTOR
    def dynamo_to_json(dicttopopulate,my_dict):
        for k,v in my_dict.items():


            value_type = list(my_dict[k].keys())[0]

            if value_type == "M":
                value = my_dict[k][value_type]

                # for i in range(0,len(value)):
                dynamodb_item_m = dict()
                dynamo_to_json(dynamodb_item_m,value)
                #     v = dynamodb_item_m

                value.update(dynamodb_item_m)
                dicttopopulate.update({k:value})

            elif value_type == "S":
                value = my_dict[k][value_type]
                dicttopopulate.update({k:value})

            elif value_type == "L": # list
                value = my_dict[k][value_type]

                new_item_list = []
                new_item_list.clear()



                for i in range(0,len(value)):

                    dynamodb_item_list = dict()
                    dynamodb_item_list.clear()

                    dynamo_to_json(dynamodb_item_list,value[i])

                    new_item_list.append(dynamodb_item_list)

                dicttopopulate.update({k:new_item_list})

            elif k == "M":

                dynamodb_item_m = dict()
                dynamo_to_json(dynamodb_item_m,v)
                v = dynamodb_item_m
                dicttopopulate.update(v)



    # DynamoDB Put Item // Create record of list translation
    def create_content_record(event_to_dynamo_json):

        try:
            response = db_client.put_item(TableName=content_table_name,Item=event_to_dynamo_json)
        except Exception as e:
            msg = "Unable to create/update item in DynamoDB, got exception:  %s" % (str(e).upper())
            LOGGER.error(msg)
            exceptions.append(msg)
            return exceptions
        return response


    # Get Item from content DB
    def get_content_record(house_id):
        try:
            key = {'house_id':{'S':house_id}}
            response = db_client.get_item(TableName=content_table_name,Key=key)
            LOGGER.debug("dynamodb get item response : %s " % (response))
        except Exception as e:
            msg = "Unable to get item information from DynamoDB, got exception:  %s " % (e)
            exceptions.append(msg)
            LOGGER.error(msg)
            return exceptions
        return response


    ## Functions End

    total_runtime_ms = 0

    if event['type'] == "ProgramEvent":

        # Create unique house id from original house id and cumulative runtime of each segment
        original_house_id = event['house_id']
        LOGGER.info("Original house id : %s " % (original_house_id))

        LOGGER.info("Iterating through segments to get cumulative list for the new house ID suffix")
        for segment_number in list(event['segments']):
            segment = event['segments'][segment_number]
            segment_runtime = segment['end_ms'] - segment['start_ms']

            total_runtime_ms += segment_runtime

        house_id = "%s_%s" % (original_house_id,total_runtime_ms)
        LOGGER.info("Total clip runtime is %s, new house id is : %s " % (total_runtime_ms,house_id))

    else: # This is NonProgramEvent

        # Create unique house id from original house id and cumulative runtime of each segment
        original_house_id = event['house_id']
        LOGGER.info("Original house id : %s " % (original_house_id))

        for adavail in event['ad_avail_detail']:

            LOGGER.info("Iterating through segments of house id %s to get cumulative list for the new house ID suffix" % (adavail['house_id']))
            for segment_number in list(adavail['segments']):
                segment = adavail['segments'][segment_number]
                segment_runtime = segment['end_ms'] - segment['start_ms']

                total_runtime_ms += segment_runtime

        house_id = "%s_%s" % (original_house_id,total_runtime_ms)
        LOGGER.info("Total clip runtime is %s, new house id is : %s " % (total_runtime_ms,house_id))

    # See if this item exists in the database
    LOGGER.info("Checking to see if item exists in the database")

    get_item_response = get_content_record(house_id)

    if len(exceptions) > 0:

        raise Exception(exceptions)

    if 'Item' not in list(get_item_response.keys()):

        LOGGER.info("Content does not exist in DB, sending through video pipeline")

        event['house_id'] = house_id
        event['workflow_state']['db_check'] = "DoesNotExist" # DoesNotExist or Exists

        # convert event json to dynamodb
        event_to_dynamo_json = dict()
        json_to_dynamo(event_to_dynamo_json,event)

        # create item in db
        create_record_response = create_content_record(event_to_dynamo_json)

        # exceptions
        if len(exceptions) > 0:
            raise Exception(exceptions)

        return event



    else: # Item exists

        LOGGER.info("Content exists in DB, nothing to do")

        event['house_id'] = house_id
        event['workflow_state']['db_check'] = "Exists" # DoesNotExist or Exists

        return event



    '''
    
    {
      "segments": {
        "1": {
          "start_ms": 0,
          "end_ms": 21734
        },
        "2": {
          "start_ms": 145567,
          "end_ms": 149501
        }
      },
      "total_segments": 2,
      "s3": "s3://aviator-imagine-tv-assets/SKYFALL_NEW.mxf",
      "framerate": 29.970029970029973,
      "type": "ProgramEvent",
      "house_id": "SKYFALL_NEW",
      "workflow_state": {
        "db_check": "na"
      }
    }
    
    
    
    '''





    '''
    try:
        # Path in API call
        path = event['path']

        # Request method
        request_method = event['httpMethod']

    except Exception as e:
        LOGGER.error("Unable to extract url path, request method, or body from request payload")
        response_json = {"status":"Unable to extract url path, request method, or body from request payload -a: %s " % (e)}
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
            response_json = {"status":"Unable to extract body from request payload: %s " % (e)}
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
        create_api_req_record(request_uuid, channel_name)

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

        request_information_json = dict()
        if "Item" in request_status:
            dynamo_to_json(request_information_json,request_status['Item'])
        else:
            request_information_json["records_found"] = 0

        response_json = {"status":request_information_json}
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
    
    '''

    #return event

    # parse event


