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

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


cdn_base_url = 'https://' + os.environ['CDN_DOMAIN_NAME']
slate_mp4 = os.environ['SLATE_URI']
emc_role_arn = os.environ['EMCROLE']
s3bucket = os.environ['S3BUCKET']

def lambda_handler(event, context):
    LOGGER.info(event)

    # Initialize boto3 clients
    db_client = boto3.client('dynamodb')
    emt_client = boto3.client('mediatailor')
    emc_client = boto3.client('mediaconvert')

    # get the account-specific mediaconvert endpoint for this region
    endpoints = emc_client.describe_endpoints()

    # add the account-specific endpoint to the client session
    emc_client = boto3.client('mediaconvert', endpoint_url=endpoints['Endpoints'][0]['Url'], verify=True)


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
    def create_ca_channel(channel_name,vod_source_location,vod_source_name):

        LOGGER.info("Initializing function: create channel assembly channel")

        try:
            create_channel_response = emt_client.create_channel(
                ChannelName=channel_name,
                FillerSlate={
                    'SourceLocationName': vod_source_location,
                    'VodSourceName': vod_source_name
                },
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

    # Create transcode
    def create_job(source_s3_uri,job_output):

        LOGGER.info("Initializing job creation function")

        try:
            emc_response = emc_client.create_job(
                AccelerationSettings={
                    "Mode": "PREFERRED"
                },
                Role=emc_role_arn,
                Tags={
                    "icautomation": "icautomation"
                },
                Settings={
                    "TimecodeConfig": {
                        "Source": "ZEROBASED"
                    },
                    "Inputs": [{
                        "TimecodeSource": "ZEROBASED",
                        "VideoSelector": {},
                        "AudioSelectors": {
                            "Audio Selector 1": {
                                "DefaultSelection": "DEFAULT"
                            }
                        },
                        "FileInput": source_s3_uri,
                        "CaptionSelectors": {
                            "Captions Selector 1": {
                                "SourceSettings": {
                                    "SourceType": "EMBEDDED",
                                    "EmbeddedSourceSettings": {}
                                }
                            }
                        },
                        "InputClippings": []
                    }],
                    "OutputGroups":[{
                        "Name": "Apple HLS",
                        "OutputGroupSettings": {
                            "Type": "HLS_GROUP_SETTINGS",
                            "HlsGroupSettings": {
                                "SegmentLength": 6,
                                "MinSegmentLength": 0,
                                "Destination": job_output,
                                "SegmentControl": "SEGMENTED_FILES",
                                "MinFinalSegmentLength": 1
                            }
                        },
                        "Outputs": [
                            {
                                "VideoDescription": {
                                    "CodecSettings": {
                                        "Codec": "H_264",
                                        "H264Settings": {
                                            "RateControlMode": "QVBR",
                                            "SceneChangeDetect": "TRANSITION_DETECTION",
                                            "MaxBitrate": 2000000,
                                            "QualityTuningLevel": "SINGLE_PASS_HQ"
                                        }
                                    },
                                    "Width": 1280,
                                    "Height": 720,
                                    "VideoPreprocessors": {
                                        "Deinterlacer": {
                                            "Mode": "ADAPTIVE",
                                            "Algorithm": "INTERPOLATE"
                                        }
                                    }
                                },
                                "AudioDescriptions": [
                                    {
                                        "CodecSettings": {
                                            "Codec": "AAC",
                                            "AacSettings": {
                                                "Bitrate": 96000,
                                                "CodingMode": "CODING_MODE_2_0",
                                                "SampleRate": 48000
                                            }
                                        },
                                        "AudioSourceName": "Audio Selector 1"
                                    }
                                ],
                                "OutputSettings": {
                                    "HlsSettings": {
                                        "SegmentModifier": "_720_"
                                    }
                                },
                                "ContainerSettings": {
                                    "Container": "M3U8",
                                    "M3u8Settings": {}
                                },
                                "NameModifier": "_720p",
                                "CaptionDescriptions": [
                                    {
                                        "DestinationSettings": {
                                            "DestinationType": "EMBEDDED"
                                        },
                                        "CaptionSelectorName": "Captions Selector 1"
                                    }
                                ]
                            },
                            {
                                "VideoDescription": {
                                    "CodecSettings": {
                                        "Codec": "H_264",
                                        "H264Settings": {
                                            "RateControlMode": "QVBR",
                                            "SceneChangeDetect": "TRANSITION_DETECTION",
                                            "MaxBitrate": 1000000,
                                            "QualityTuningLevel": "SINGLE_PASS_HQ"
                                        }
                                    },
                                    "Width": 960,
                                    "Height": 540,
                                    "VideoPreprocessors": {
                                        "Deinterlacer": {
                                            "Mode": "ADAPTIVE",
                                            "Algorithm": "INTERPOLATE"
                                        }
                                    }
                                },
                                "AudioDescriptions": [
                                    {
                                        "CodecSettings": {
                                            "Codec": "AAC",
                                            "AacSettings": {
                                                "Bitrate": 96000,
                                                "CodingMode": "CODING_MODE_2_0",
                                                "SampleRate": 48000
                                            }
                                        }
                                    }
                                ],
                                "OutputSettings": {
                                    "HlsSettings": {
                                        "SegmentModifier": "_540_"
                                    }
                                },
                                "ContainerSettings": {
                                    "Container": "M3U8",
                                    "M3u8Settings": {}
                                },
                                "NameModifier": "_540p",
                                "CaptionDescriptions": [
                                    {
                                        "DestinationSettings": {
                                            "DestinationType": "EMBEDDED"
                                        },
                                        "CaptionSelectorName": "Captions Selector 1"
                                    }
                                ]
                            },
                            {
                                "VideoDescription": {
                                    "CodecSettings": {
                                        "Codec": "H_264",
                                        "H264Settings": {
                                            "RateControlMode": "QVBR",
                                            "SceneChangeDetect": "TRANSITION_DETECTION",
                                            "QualityTuningLevel": "SINGLE_PASS_HQ",
                                            "MaxBitrate": 750000
                                        }
                                    },
                                    "Width": 640,
                                    "Height": 360,
                                    "VideoPreprocessors": {
                                        "Deinterlacer": {
                                            "Mode": "ADAPTIVE",
                                            "Algorithm": "INTERPOLATE"
                                        }
                                    }
                                },
                                "AudioDescriptions": [
                                    {
                                        "CodecSettings": {
                                            "Codec": "AAC",
                                            "AacSettings": {
                                                "Bitrate": 96000,
                                                "CodingMode": "CODING_MODE_2_0",
                                                "SampleRate": 48000
                                            }
                                        }
                                    }
                                ],
                                "OutputSettings": {
                                    "HlsSettings": {}
                                },
                                "ContainerSettings": {
                                    "Container": "M3U8",
                                    "M3u8Settings": {}
                                },
                                "NameModifier": "_360p",
                                "CaptionDescriptions": [
                                    {
                                        "DestinationSettings": {
                                            "DestinationType": "EMBEDDED"
                                        },
                                        "CaptionSelectorName": "Captions Selector 1"
                                    }
                                ]
                            },
                            {
                                "VideoDescription": {
                                    "CodecSettings": {
                                        "Codec": "H_264",
                                        "H264Settings": {
                                            "RateControlMode": "QVBR",
                                            "SceneChangeDetect": "TRANSITION_DETECTION",
                                            "QualityTuningLevel": "SINGLE_PASS_HQ",
                                            "MaxBitrate": 500000
                                        }
                                    },
                                    "Width": 480,
                                    "Height": 270,
                                    "VideoPreprocessors": {
                                        "Deinterlacer": {
                                            "Mode": "ADAPTIVE",
                                            "Algorithm": "INTERPOLATE"
                                        }
                                    }
                                },
                                "AudioDescriptions": [
                                    {
                                        "CodecSettings": {
                                            "Codec": "AAC",
                                            "AacSettings": {
                                                "Bitrate": 96000,
                                                "CodingMode": "CODING_MODE_2_0",
                                                "SampleRate": 48000
                                            }
                                        },
                                        "AudioSourceName": "Audio Selector 1"
                                    }
                                ],
                                "OutputSettings": {
                                    "HlsSettings": {
                                        "SegmentModifier": "_500_"
                                    }
                                },
                                "ContainerSettings": {
                                    "Container": "M3U8",
                                    "M3u8Settings": {}
                                },
                                "NameModifier": "_270p",
                                "CaptionDescriptions": [
                                    {
                                        "DestinationSettings": {
                                            "DestinationType": "EMBEDDED"
                                        },
                                        "CaptionSelectorName": "Captions Selector 1"
                                    }
                                ]
                            }
                        ],
                        "CustomName": "HLS"
                    }]
                }
            )
            return emc_response
        except Exception as e:
            msg = "Unable to create job, got exception : %s " % (e)
            LOGGER.error(msg)
            exceptions.append(msg)
            return msg





    ### FUNCTIONS

    # Check if channel exists
    # DO LATER

    # 1. Transcode the slate
    # 2. Create Source Location
    # 3. Create Slate source
    # 4. Create channel
    # 5. update API req DB

    #
    # Transcode Filler Slate
    #

    source_s3_uri = "s3://%s/%s" % (s3bucket,slate_mp4)
    job_output = "s3://%s/ChannelAssembly/%s/%s" % (s3bucket,"SLATE","SLATE")

    emc_create_response = create_job(source_s3_uri,job_output)
    emc_create_response_json = json.loads(json.dumps(emc_create_response, default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"))

    if len(exceptions) > 0:

        LOGGER.error("Could not create job")
        raise Exception(exceptions)



    #
    # Transcode Filler Slate - end
    #

    #
    # CREATE SOURCE LOCATION AND FILLER SLATE SOURCE
    #

    # Get source locations and see if CDN already exists
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
    vod_source_name = "SLATE"
    vod_source_location = "/ChannelAssembly/SLATE/SLATE.m3u8"

    get_source_response = get_source(cdn_name,vod_source_name)

    if len(get_source_response['VodSourceName']) < 1:

        # Create VOD Source
        source_create_response = create_source(vod_source_name,vod_source_location,cdn_name)

        source_create_response_json = emc_response_json = json.loads(json.dumps(source_create_response, default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"))

        if len(exceptions) > 1:
            raise Exception(exceptions)s

        LOGGER.info("Created VOD Source")

        event['workflow_state'] = {"mediatailor_source": "created"}

    else:

        LOGGER.info("Vod Source already exists, nothing to do")

        event['workflow_state'] = {"mediatailor_source": "already exists"}


    #
    # CREATE SOURCE END
    #

    channel_name = event['list']['PlayoutChannel']
    # Create channel

    create_ca_channel(channel_name,cdn_name,vod_source_name)
    add_channel_policy(channel_name)


    # Start channel
    # Dont do this here, do it at the end of program creation

    # GET API REQ Status
    request_id = event['request']

    # UPDATE API Req status with channel create

    return event