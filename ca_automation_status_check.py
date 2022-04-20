import json
import logging
import uuid
import boto3
import base64
import datetime
import os
import time
import math

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
content_table_name = os.environ['CONTENTDB']
api_req_table_name = os.environ['APIREQDB']
#emc_role_arn = os.environ['EMCROLE']
#s3bucket = os.environ['S3BUCKET']



def lambda_handler(event, context):
    LOGGER.info(event)

    # Initialize boto3 clients
    emc_client = boto3.client('mediaconvert')
    db_client = boto3.client('dynamodb')


    # get the account-specific mediaconvert endpoint for this region
    endpoints = emc_client.describe_endpoints()

    # add the account-specific endpoint to the client session
    emc_client = boto3.client('mediaconvert', endpoint_url=endpoints['Endpoints'][0]['Url'], verify=True)

    # initialize exceptions list to capture issues and exit if necessary
    exceptions = []
    exceptions.clear()

    ### FUNCTIONS START


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


    # DynamoDB Get Item // record of list translation
    def get_api_req_record(request_uuid):

        try:
            response = db_client.get_item(TableName=api_req_table_name,Key={"request_id":{"S":request_uuid}})
        except Exception as e:
            exceptions.append("Unable to get item from database, got exception:  %s" % (str(e).upper()))
            return exceptions
        return response

    # EMC get job
    def emc_get_job(job_id):

        try:
            emc_response = emc_client.get_job(Id=job_id)
        except Exception as e:
            msg = "unable to get job info from MediaConvert, got exception : %s " % (e)
            LOGGER.error(msg)
            exceptions.append(msg)
            return msg

        return emc_response



    #
    # FUNCTIONS END
    #

    # Get MediaConvert Status

    emc_job_id = event['workflow_state']['transcode']
    house_id = event['house_id']

    emc_response = emc_get_job(emc_job_id)

    if len(exceptions) > 0:
        raise Exception(exceptions)


    emc_response_json = json.loads(json.dumps(emc_response, default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"))

    job_status = emc_response_json['Job']['Status'] # 'Status': 'SUBMITTED'|'PROGRESSING'|'COMPLETE'|'CANCELED'|'ERROR',
    output_path = emc_response_json['Job']['Settings']['OutputGroups'][0]['OutputGroupSettings']['HlsGroupSettings']['Destination']

    LOGGER.info("MediaConvert job status : %s " % (job_status) )

    # Update event
    event['workflow_state']['transcode_status'] = job_status

    if job_status == "COMPLETE":

        # $.workflow_state.transcode_complete
        event['output_path'] = output_path
        event['workflow_state']['transcode_complete'] = True

        return event

    elif job_status == "ERROR":


        # $.workflow_state.transcode_complete = FALSE
        event['workflow_state']['transcode_complete'] = False

        raise Exception("Transcode failed")

    else:

        # $.workflow_state.transcode_complete = FALSE
        event['workflow_state']['transcode_complete'] = False


    # Get DB Item
    get_record_response = get_content_record(house_id)['Item']

    json_db_item = dict()

    dynamo_to_json(json_db_item,get_record_response)

    json_db_item['workflow_state']['transcode_status'] = job_status
    json_db_item['output_path'] = output_path

    # Update DB
    dynamo_db_item = dict()

    json_to_dynamo(dynamo_db_item,json_db_item)

    update_item_response = create_content_record(dynamo_db_item)

    if len(exceptions) > 0:
        raise Exception(exceptions)

    LOGGER.info("Successfully updated db item : %s " % (event))

    return event