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

def lambda_handler(event, context):
    LOGGER.info(event)

    # Initialize boto3 clients
    db_client = boto3.client('dynamodb')
    emt_client = boto3.client('mediatailor')


    # initialize exceptions list to capture issues and exit if necessary
    exceptions = []
    exceptions.clear()

    ### FUNCTIONS

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

    # Create channel
    def create_ca_channel(channel_name):

        LOGGER.info("Initializing function: create channel assembly channel")

        try:
            create_channel_response = emt_client.create_channel(
                ChannelName=channel_name,
                Outputs=[
                    {
                        'HlsPlaylistSettings': {
                            'ManifestWindowSeconds': 30
                        },
                        'ManifestName': 'index',
                        'SourceGroup': 'sg1'
                    },
                ],
                PlaybackMode='LINEAR',
                Tags={
                    'icautomation': 'icautomation'
                }
            )
        except Exception as e:
            msg = "Unable to create MediaTailor CA Channel, got exception : %s " % (e)
            LOGGER.error(msg)
            exceptions.append(msg)
            return msg
        return create_channel_response

    def add_channel_policy(channel_name):

        LOGGER.info("Initializing function: add channel policy")

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowAnonymous",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "mediatailor:GetManifest",
                    "Resource": "arn:aws:mediatailor:us-west-2:301520684698:channel/e"
                }
            ]
        }

        try:

            add_policy_response = emt_client.put_channel_policy(ChannelName=channel_name,Policy=policy)

        except Exception as e:
            msg = "Could not add policy to channel, got exception : %s " % (e)
            LOGGER.error(msg)
            exceptions.append(msg)
            return msg
        return add_policy_response


    ### FUNCTIONS

    # Check if channel exists
    # DO LATER

    channel_name = event['list']['PlayoutChannel']
    # Create channel

    create_channel_response(channel_name)
    add_channel_policy(channel_name)


    # Start channel
    # Dont do this here, do it at the end of program creation

    # GET API REQ Status
    request_id = event['request']

    # UPDATE API Req status with channel create

    return event