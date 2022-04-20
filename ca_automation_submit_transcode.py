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
emc_role_arn = os.environ['EMCROLE']
s3bucket = os.environ['S3BUCKET']



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

    def create_job(job_input,job_output):

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
                        "Source": "EMBEDDED"
                    },
                    "Inputs": job_input,
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

    ## Create EMC job


    ### FUNCTIONS END


    job_input = []

    input_template = {
        "TimecodeSource": "EMBEDDED",
        "VideoSelector": {},
        "AudioSelectors": {
            "Audio Selector 1": {
                "DefaultSelection": "DEFAULT"
            }
        },
        "FileInput": "s3://source-bucket/path/path/path/asset.mxf",
        "CaptionSelectors": {
            "Captions Selector 1": {
                "SourceSettings": {
                    "SourceType": "EMBEDDED",
                    "EmbeddedSourceSettings": {}
                }
            }
        },
        "InputClippings": []
    }



    if event['type'] == 'ProgramEvent':
        # This will be a single input with possibly multiple trim points

        LOGGER.info("Clip is a ProgramEvent")

        input_clippings_list = []

        for segment_number in event['segments']:


            segment = event['segments'][segment_number]

            start_ms = float(int(segment['start_ms']) / 1000)
            end_ms = float(int(segment['end_ms']) / 1000)


            start_timecode_hhmmss = datetime.datetime.fromtimestamp(start_ms).strftime('%H:%M:%S')
            start_frames = math.floor(event['framerate'] * (int(datetime.datetime.fromtimestamp(start_ms).strftime('%f')[0:3]) / 1000 ))
            end_timecode_hhmmss = datetime.datetime.fromtimestamp(end_ms).strftime('%H:%M:%S')
            end_frames = math.floor(event['framerate'] * (int(datetime.datetime.fromtimestamp(end_ms).strftime('%f')[0:3]) / 1000 ))

            start_timecode_frames = "%s;%02d" % (start_timecode_hhmmss,start_frames)
            end_timecode_frames = "%s;%02d" % (end_timecode_hhmmss,end_frames)

            clipping = {
                "StartTimecode": start_timecode_frames,
                "EndTimecode": end_timecode_frames
            }

            LOGGER.info("Segment clipping details : %s " % (clipping))


            input_clippings_list.append(clipping)

        input_template['FileInput'] = event['s3']
        input_template['InputClippings'] = input_clippings_list


        LOGGER.info("Job input : %s " % (input_template))
        job_input.append(input_template)


    else:

        LOGGER.info("Clip is a NonProgramEvent")

        for avail in event['ad_avail_detail']:

            input_clippings_list = []

            for segment_number in avail['segments']:

                segment = avail['segments'][segment_number]

                start_ms = float(int(segment['start_ms']) / 1000)
                end_ms = float(int(segment['end_ms']) / 1000)


                start_timecode_hhmmss = datetime.datetime.fromtimestamp(start_ms).strftime('%H:%M:%S')
                start_frames = math.floor(avail['framerate'] * (int(datetime.datetime.fromtimestamp(start_ms).strftime('%f')[0:3]) / 1000 ))
                end_timecode_hhmmss = datetime.datetime.fromtimestamp(end_ms).strftime('%H:%M:%S')
                end_frames = math.floor(avail['framerate'] * (int(datetime.datetime.fromtimestamp(end_ms).strftime('%f')[0:3]) / 1000 ))

                start_timecode_frames = "%s;%02d" % (start_timecode_hhmmss,start_frames)
                end_timecode_frames = "%s;%02d" % (end_timecode_hhmmss,end_frames)

                clipping = {
                    "StartTimecode": start_timecode_frames,
                    "EndTimecode": end_timecode_frames
                }

                LOGGER.info("Segment clipping details : %s " % (clipping))


                input_clippings_list.append(clipping)

            input_template_copy = dict(input_template)
            input_template_copy['FileInput'] = avail['s3']
            input_template_copy['InputClippings'] = input_clippings_list


            LOGGER.info("Job input : %s " % (input_template_copy))
            job_input.append(input_template_copy)


    #
    # Create the MediaConvert job
    #

    job_output = "s3://%s/ChannelAssembly/%s/%s" % (s3bucket,event['house_id'],event['house_id'])

    emc_create_response = create_job(job_input,job_output)
    emc_create_response_json = json.loads(json.dumps(emc_create_response, default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"))

    if len(exceptions) > 0:

        LOGGER.error("Could not create job")
        raise Exception(exceptions)


    # get job id

    emc_job_id = emc_create_response_json['Job']['Id']

    LOGGER.info("MediaConvert job created successfully, id : %s " % (emc_job_id))


    # Update Content DB

    # Get record from database
    LOGGER.info("Getting item record from database")

    house_id = event['house_id']

    get_item_response = get_content_record(house_id)['Item']

    # dynamo to json
    json_db_item = dict()
    dynamo_to_json(json_db_item,get_item_response)

    if len(exceptions) > 0:

        LOGGER.error("Could not get content record from database")
        raise Exception(exceptions)

    json_db_item['workflow_state']['db_check'] = "Exists"
    event['workflow_state']['db_check'] = "Exists"
    json_db_item['workflow_state']['transcode'] = emc_job_id
    event['workflow_state']['transcode'] = emc_job_id

    # convert json back to dynamo json
    dynamo_db_item = dict()
    json_to_dynamo(dynamo_db_item,json_db_item)

    # Update DB record
    create_content_record(dynamo_db_item)

    if len(exceptions) > 0:

        LOGGER.error("Could not update content record in database")
        raise Exception(exceptions)



    # Update Event workflow state to include transcoding

    return event


{
    "house_id": "ABC78623SH-SHVY0008000H",
    "type": "NonProgramEvent",
    "ad_avail_detail": [
        {
            "segments": {
                "0": {
                    "start_ms": 0,
                    "end_ms": 30000
                }
            },
            "total_segments": 0,
            "s3": "s3://aviator-imagine-tv-assets/ABC78623SH.mxf",
            "framerate": 29.970029970029973,
            "type": "NonProgramEvent",
            "house_id": "ABC78623SH",
            "workflow_state": {
                "db_check": "na"
            }
        },
        {
            "segments": {
                "0": {
                    "start_ms": 0,
                    "end_ms": 30000
                }
            },
            "total_segments": 0,
            "s3": "s3://aviator-imagine-tv-assets/SHVY0008000H.mxf",
            "framerate": 29.970029970029973,
            "type": "NonProgramEvent",
            "house_id": "SHVY0008000H",
            "workflow_state": {
                "db_check": "na"
            }
        }
    ]
}


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
    "house_id": "SKYFALL_NEW_25668",
    "workflow_state": {
        "db_check": "DoesNotExist"
    }
}

'''



  "Settings": {
    "Inputs": [
      {
        "TimecodeSource": "EMBEDDED",
        "VideoSelector": {},
        "AudioSelectors": {
          "Audio Selector 1": {
            "DefaultSelection": "DEFAULT"
          }
        },
        "FileInput": "s3://source-bucket/path/path/path/asset.mxf",
        "CaptionSelectors": {
          "Captions Selector 1": {
            "SourceSettings": {
              "SourceType": "EMBEDDED",
              "EmbeddedSourceSettings": {}
            }
          }
        },
        "InputClippings": [
          {
            "StartTimecode": "00:00:00:00",
            "EndTimecode": "00:00:00:01"
          }
        ]
      }
    ],
    "OutputGroups": [
      {
        "Name": "Apple HLS",
        "OutputGroupSettings": {
          "Type": "HLS_GROUP_SETTINGS",
          "HlsGroupSettings": {
            "SegmentLength": 6,
            "MinSegmentLength": 0,
            "Destination": "s3://output-bucket/path/path/path/houseid",
            "SegmentControl": "SEGMENTED_FILES",
            "MinFinalSegmentLength": 1,
            "CaptionSegmentLengthControl": "MATCH_VIDEO"
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
      }
    ],
    "TimecodeConfig": {
      "Source": "EMBEDDED"
    }
  },
  "Role": "arn:aws:iam::301520684698:role/service-role/MediaConvert_Default_Role",
  "StatusUpdateInterval": "SECONDS_12",
  "Queue": "arn:aws:mediaconvert:us-west-2:301520684698:queues/Default",
  "UserMetadata": {
    "icaws": "versioautomationworkflow"
  }
}



'''