import logging
import uuid
import boto3
import base64
import datetime
import os
import time
import math
import json
from urllib.parse import urlparse
import dateutil.parser as dp

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3bucket = os.environ['S3BUCKET']
cdn_base_url = 'https://' + os.environ['CDN_DOMAIN_NAME']

def lambda_handler(event, context):
    LOGGER.info(event)

    # Initialize boto3 clients
    db_client = boto3.client('dynamodb')
    emt_client = boto3.client('mediatailor')

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


    def create_program(item, cdn_name):
        LOGGER.info("Initializing function: create emt program")

        try:

            create_program_response = emt_client.create_program(
                AdBreaks=item['AdBreaks'],
                ChannelName=item['ChannelName'],
                ProgramName=item['ProgramName'], # + "_" + str(item['ScheduleConfiguration']['Transition']['ScheduledStartTimeMillis']),
                ScheduleConfiguration=item['ScheduleConfiguration'],
                SourceLocationName=cdn_name,
                VodSourceName=item['ProgramName'],
            )
            LOGGER.info("Successfully created Program")

        except Exception as e:
            msg = "Unable to create program, got exception : %s " % (e)
            LOGGER.error(msg)
            exceptions.append(msg)
            return msg
        return create_program_response


    def get_req_data(bucket,key):
        LOGGER.info("Attempting to get request data json from S3: %s " % (key))

        # s3 boto3 client initialize
        s3_client = boto3.client('s3')

        try:
            s3_raw_response = s3_client.get_object(Bucket=bucket,Key=key)
        except Exception as e:
            msg = "Unable to get template %s from S3, got exception : %s" % (key,e)
            LOGGER.error(msg)
            exceptions.append(msg)
            return msg

        return json.loads(s3_raw_response['Body'].read())

    def put_req_data(bucket,key,s3_data):
        LOGGER.info("Attempting to update request data json with new data")
        content_type = "application/json"

        # s3 boto3 client initialize
        s3_client = boto3.client('s3')

        try:
            s3_response = s3_client.put_object(Body=json.dumps(s3_data), Bucket=bucket, Key=key,ContentType=content_type, CacheControl='no-cache')
            LOGGER.info("Put object to S3")
            event['status'] = "Channel map updated successfully"
        except Exception as e:
            msg = "Unable to update channel map json, got exception : %s " % (e)
            LOGGER.error(msg)
            event['status'] = msg
            exceptions.append(msg)


    def hms_to_s(input_hms):
        h, m, sms = input_hms.split(':')
        s = sms.split(frame_separator)[0]
        frames = sms.split(frame_separator)[1][0:2]
        total_ms = round(int(frames) / frame_rate,3) * 1000

        total_seconds = int(h) * 3600 + int(m) * 60 + int(s)

        return {"s":total_seconds,"ms":total_ms}



    ### FUNCTIONS END


    # Get request payload from S3 and put into event key
    s3bucket = event['list_location'].split("/")[2]
    s3key = '/'.join(event['list_location'].split("/")[3:])
    request_list = json.loads(get_req_data(s3bucket,s3key))
    if len(exceptions) > 0:
        raise Exception(exceptions)
    event['list'] = request_list


    frame_rate = event['list']['Framerate']
    frame_separator = ""

    if isinstance(frame_rate, float) and frame_rate > 24:
        # This is drop_frame
        frame_separator = ";"
    else:
        frame_separator = ":"


    channel_name = event['list']['PlayoutChannel']

    start_date_iso = event['list']['ScheduleStart']['Date']
    start_time_hms = event['list']['ScheduleStart']['Timecode']

    parsed_start_date = dp.parse(start_date_iso)
    start_date_epoch = int(parsed_start_date.strftime('%s'))
    start_s_and_ms = hms_to_s(start_time_hms)
    start_epochms = (start_date_epoch + start_s_and_ms['s']) * 1000 + int(start_s_and_ms['ms'])

    current_epochms = int(time.time())*1000

    schedule_start_offset = 0
    if current_epochms - start_epochms > 0:
        # adjust for start in 2 minutes from now
        schedule_start_offset = (current_epochms - start_epochms) + 120000



    ca_scheduled_items = event['channel_assembly_schedule']


    cdn_name = urlparse(cdn_base_url).netloc.replace(".","_")

    programs_for_channel = []

    for item_number in range(0,len(ca_scheduled_items)):

        item = ca_scheduled_items[item_number]

        # if item_number > 0:
        #     prg_name = ca_scheduled_items[item_number-1]['ProgramName'] + "_" + str(ca_scheduled_items[item_number-1]['ScheduleConfiguration']['Transition']['ScheduledStartTimeMillis'])
        # else:
        #     prg_name = ""

        # # Adjust start time if needed
        item['ScheduleConfiguration']['Transition']['ScheduledStartTimeMillis'] += schedule_start_offset
        # item['ScheduleConfiguration']['Transition']['RelativePosition'] = "AFTER_PROGRAM"
        # item['ScheduleConfiguration']['Transition']['RelativeProgram'] = prg_name

        LOGGER.info("Creating Program: %s" % (item['ProgramName']))
        create_program(item,cdn_name)

        programs_for_channel.append(item)


    if len(exceptions) > 0:

        LOGGER.warning("exceptions while creating programs : %s " % (exceptions))

    return programs_for_channel
