import json
import logging
import uuid
import boto3
import base64
import datetime
import os
from urllib.parse import urlparse


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

cdn_base_url = 'https://' + os.environ['CDN_DOMAIN_NAME']

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

    # DYNAMO DB JSON BUILDER
    def json_to_dynamo(dicttopopulate,my_dict):
        for k,v in my_dict.items():

            if isinstance(v,dict):
                dynamodb_item_subdict = dict()
                json_to_dynamo(dynamodb_item_subdict,v)

                v = dynamodb_item_subdict
                dicttopopulate.update({k:{"M":v}})

            elif isinstance(v,str):
                dicttopopulate.update({k:{"S":v}})
            elif isinstance(v,list):
                for i in range(0,len(v)):
                    dynamodb_item_list = dict()
                    json_to_dynamo(dynamodb_item_list,v[i])

                    v[i] = {"M":dynamodb_item_list}

                dicttopopulate.update({k:{"L":v}})

    def dynamo_to_json(dicttopopulate,my_dict):
        for k,v in my_dict.items():

            value_type = list(my_dict[k].keys())[0]

            if value_type == "M":
                value = my_dict[k][value_type]

                for i in range(0,len(value)):
                    dynamodb_item_m = dict()
                    dynamo_to_json(dynamodb_item_m,value)
                    v = dynamodb_item_m

                value.update(dynamodb_item_m)
                dicttopopulate.update({k:value})

            elif value_type == "S":
                value = my_dict[k][value_type]
                dicttopopulate.update({k:value})

            elif value_type == "L": # list
                value = my_dict[k][value_type]

                for i in range(0,len(value)):
                    dynamodb_item_list = dict()
                    dynamo_to_json(dynamodb_item_list,value[i])

                    value[i] = dynamodb_item_list

                dicttopopulate.update({k:value})
            elif k == "M":

                dynamodb_item_m = dict()
                dynamo_to_json(dynamodb_item_m,v)
                v = dynamodb_item_m
                dicttopopulate.update(v)

    # DynamoDB Get Item // record of list translation
    def get_api_req_record(request_uuid):

        api_request_database = os.environ['APIREQDB']

        try:
            response = db_client.get_item(TableName=api_request_database,Key={"request_id":{"S":request_uuid}})
        except Exception as e:
            exceptions.append("Unable to get item from database, got exception:  %s" % (str(e).upper()))
            return exceptions
        return response


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

    # Get source location
    def list_source_locations():
        LOGGER.info("Initializing function: source locations list")
        try:
            response = emt_client.list_source_locations(MaxResults=100)
        except Exception as e:
            msg = "Unable to get source locations, got exception: %s " % (e)
            LOGGER.error(msg)
            exceptions.append
            return msg
        return response

    # Create source location
    def create_source_location(cdn_base_url,cdn_name):

        try:
            create_location_response = emt_client.create_source_location(HttpConfiguration={'BaseUrl': cdn_base_url},SourceLocationName=cdn_name,Tags={"icautomation": "icautomation"})
            LOGGER.info("Successfully created VOD source location")
        except Exception as e:
            msg = "Unable to create source location, got exception : %s " % (e)
            exceptions.append(msg)
            LOGGER.warning(msg)
            return msg

        return create_location_response

    # Get source
    def get_source(source_location_name,vod_source_name):

        LOGGER.info("Initializing function: MediaTailor describe vod source")
        LOGGER.info("SourceLocationName: %s , VodSourceName: %s" % (source_location_name,vod_source_name))

        try:
            response = emt_client.describe_vod_source(SourceLocationName=source_location_name,VodSourceName=vod_source_name)
        except Exception as e:
            msg = "Unable to get VOD source from MediaTailor API, got exception: %s " % (e)
            LOGGER.error(msg)
            exceptions.append(msg)
            return {'VodSourceName': ''}

        return response

    # Create source
    def create_source(vod_source_name,vod_source_location,cdn_name):

        LOGGER.info("Initializing function: MediaTailor create vod source")

        try:
            create_vod_source_response = emt_client.create_vod_source(HttpPackageConfigurations=[{'Path': vod_source_location,'SourceGroup': 'sg1','Type':'HLS'}],SourceLocationName=cdn_name,Tags={'icautomation': 'icautomation'},VodSourceName=vod_source_name)
        except Exception as e:
            msg = "Unable to create VOD source, got exception : %s" % (e)
            LOGGER.error(msg)
            exceptions.append(msg)
            return msg
        return create_vod_source_response

    ## Functions End


    #
    # Get source locations and see if CDN already exists
    #
    source_locations = list_source_locations()['Items']
    source_locations_json = emc_response_json = json.loads(json.dumps(source_locations, default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"))

    source_location_exists = False

    cdn_name = urlparse(cdn_base_url).netloc.replace(".","_")

    LOGGER.info("Source location for workflow : %s" % (cdn_base_url))

    if len(source_locations_json) > 0:
        for sl in source_locations_json:
            base_url = sl['HttpConfiguration']['BaseUrl']
            LOGGER.info("Existing location : %s " % (base_url))

            if base_url.strip() == cdn_base_url.strip():

                source_location_exists = True

    if source_location_exists is False:

        LOGGER.info("Source location doesnt exist, trying to create")

        # Create source location
        create_source_location(cdn_base_url,cdn_name)


        # If there's an error we will ignore it as there could be a race condition happening

    else:
        LOGGER.info("Source location already exists")


    # Check if VOD Source exists
    vod_source_name = '_'.join(event['house_id'].split("_")[0:-1])

    vod_source_location = '/'+'/'.join(event['output_path'].rsplit("/",3)[1:]) + ".m3u8"

    get_source_response = get_source(cdn_name,vod_source_name)

    if len(get_source_response['VodSourceName']) < 1:

        # Create VOD Source
        LOGGER.info("Creating VOD Source : %s " % (vod_source_name))
        source_create_response = create_source(vod_source_name,vod_source_location,cdn_name)

        source_create_response_json = emc_response_json = json.loads(json.dumps(source_create_response, default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"))

        # if len(exceptions) > 1:
        #     raise Exception(exceptions)

        LOGGER.info("Created VOD Source")

        event['workflow_state']['mediatailor_source'] = "created"

    else:

        LOGGER.info("Vod Source already exists, nothing to do")

        event['workflow_state']['mediatailor_source'] = "already exists"

    return event