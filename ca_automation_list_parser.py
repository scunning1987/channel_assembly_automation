import json
import logging
import datetime
import dateutil.parser as dp
import os
import boto3

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3bucket = os.environ['S3BUCKET']

def lambda_handler(event, context):
    LOGGER.info(event)

    # to track exceptions
    exceptions = []
    exceptions.clear()


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

    # arguments from API handler
    # return event['detail']['request'] # request uuid
    # return event['detail']['clips'] # empty
    # return event['detail']['workflow_status']


    # Get request payload from S3 and put into event key
    s3bucket = event['detail']['list_location'].split("/")[2]
    s3key = '/'.join(event['detail']['list_location'].split("/")[3:])
    request_list = json.loads(get_req_data(s3bucket,s3key))
    if len(exceptions) > 0:
        raise Exception(exceptions)
    event['detail']['list'] = request_list


    versio_list_json = event['detail']['list']

    source_location_name = "us-west-2_mediapackage"

    channel_name = versio_list_json['PlayoutChannel']
    frame_rate = versio_list_json['Framerate']
    frame_separator = ""

    if isinstance(frame_rate, float) and frame_rate > 24:
        # This is drop_frame
        frame_separator = ";"
    else:
        frame_separator = ":"


    program_schedule = []
    #media_prep = []
    media_prep = dict() # primary keys as houseId
    ad_prep = dict() # Ad content that needs to be stitched by MediaConvert
    list_for_step_functions_map = []

    schedule_item = []

    for schedule_item in versio_list_json['ScheduledEvents']:

        segments = dict()

        content_type = schedule_item['PrimaryContent']['ContentType']
        item_type = schedule_item['Type'] # ProgramEvent = program , NonProgramEvent = ad

        if content_type == "Video":

            house_id = schedule_item['PrimaryContent']['HouseId']
            start_date_iso = schedule_item['Start']['Date']
            start_time_hms = schedule_item['Start']['Timecode']
            s3_uri = "s3://%s" % (schedule_item['PrimaryContent']['Filename'].replace("\\","/"))

            segment_number = schedule_item['PrimaryContent']['SegmentNumber']
            total_segments = schedule_item['PrimaryContent']['TotalSegments']

            if house_id not in media_prep.keys():
                media_prep[house_id] = {}

            if "segments" not in media_prep[house_id].keys():
                media_prep[house_id]['segments'] = {}

            # duration and SOM
            som_hms = schedule_item['SOM']
            duration_hms = schedule_item['Duration']
            end_mode = schedule_item['EndMode']

            duration_s_and_ms = hms_to_s(duration_hms)
            som_s_and_ms = hms_to_s(som_hms)


            if total_segments > 0: # Multi Segment

                for seg_schedule_item in versio_list_json['ScheduledEvents']:

                    for s in range(1,total_segments+1):

                        if seg_schedule_item['PrimaryContent']['HouseId'] == house_id and seg_schedule_item['PrimaryContent']['SegmentNumber'] == s:

                            # get timing and push to segment dict
                            LOGGER.info("get timing and push to segment dict")

                            # duration and SOM
                            seg_som_hms = seg_schedule_item['SOM']
                            seg_duration_hms = seg_schedule_item['Duration']
                            seg_end_mode = seg_schedule_item['EndMode']
                            seg_number = seg_schedule_item['PrimaryContent']['SegmentNumber']

                            seg_duration_s_and_ms = hms_to_s(seg_duration_hms)
                            seg_som_s_and_ms = hms_to_s(seg_som_hms)

                            seg_som_milliseconds = (seg_som_s_and_ms['s']*1000) + seg_som_s_and_ms['ms']

                            #return som_milliseconds

                            # if seg_end_mode == "Duration":
                            #     eom_milliseconds = "end"
                            # else:
                            eom_milliseconds = (seg_duration_s_and_ms['s']*1000) + seg_duration_s_and_ms['ms'] + seg_som_milliseconds

                            segments[seg_number] = {"start_ms":seg_som_milliseconds,"end_ms":eom_milliseconds}


            else: # Single segment

                LOGGER.info("Single segment")

                #{"som":(som_s_and_ms['s']*1000)+som_s_and_ms['ms'],"eom":""}

                som_milliseconds = (som_s_and_ms['s']*1000) + som_s_and_ms['ms']

                # if end_mode == "Duration":
                #     eom_milliseconds = "end"
                # else:
                eom_milliseconds = (duration_s_and_ms['s']*1000) + duration_s_and_ms['ms'] + som_milliseconds

                segments[segment_number] = {"start_ms":som_milliseconds,"end_ms":eom_milliseconds}





            media_prep[house_id]['total_segments'] = total_segments
            media_prep[house_id]['s3'] = s3_uri
            media_prep[house_id]['framerate'] = frame_rate
            media_prep[house_id]['segments'] = segments
            media_prep[house_id]['type'] = item_type
            media_prep[house_id]['house_id'] = house_id
            media_prep[house_id]['workflow_state'] = {'db_check':'na'}

    ###

    child_schedule_item = []

    adbreaks = dict()
    scheduled_events = versio_list_json['ScheduledEvents']
    break_number = 1

    for schedule_number in range(0,len(scheduled_events)):

        if schedule_number not in child_schedule_item:

            child_schedule_item.append(schedule_number)

            schedule_item = scheduled_events[schedule_number]

            content_type = schedule_item['PrimaryContent']['ContentType']
            item_type = schedule_item['Type'] # ProgramEvent = program , NonProgramEvent = ad
            house_id = schedule_item['PrimaryContent']['HouseId']



            avails_in_break = []

            if content_type == "Video":

                if item_type == "NonProgramEvent": # This is adbreak

                    ## Do a for loop through remaining items to see how many ads in this pod

                    for nested_schedule_number in range(schedule_number,len(scheduled_events)):

                        LOGGER.info("break number %s" % (break_number))

                        child_schedule_item.append(nested_schedule_number)

                        schedule_item = scheduled_events[nested_schedule_number]

                        child_content_type = schedule_item['PrimaryContent']['ContentType']
                        child_item_type = schedule_item['Type'] # ProgramEvent = program , NonProgramEvent = ad
                        child_house_id = schedule_item['PrimaryContent']['HouseId']

                        if child_content_type == "Video":

                            if child_item_type == "NonProgramEvent": # This is adbreak

                                # add item to list
                                avails_in_break.append(child_house_id)


                            if child_item_type == "ProgramEvent":

                                # end of adbreak or list

                                adbreaks[str(break_number)] = avails_in_break

                                break_number += 1
                                child_schedule_item.pop()

                                break

                            if nested_schedule_number == len(scheduled_events)-1:

                                adbreaks[str(break_number)] = avails_in_break

                                break_number += 1
                                child_schedule_item.pop()
                                break


    ###


    #return media_prep
    event['detail']['clips']['program_content'] = media_prep
    event['detail']['clips']['ad_breaks'] = adbreaks

    # iterate
    unique_event_list = []
    for media_key in media_prep:

        if media_prep[media_key]['type'] == "ProgramEvent":
            media_dict = dict()
            media_dict = media_prep[media_key]
            media_dict['house_id'] = media_key

            if media_dict['house_id'] not in unique_event_list:

                list_for_step_functions_map.append(media_dict)
                unique_event_list.append(media_dict['house_id'])

    for adbreak in adbreaks:

        ad_pod_name = ""
        ad_avail_detail = []
        ads_in_break = adbreaks[adbreak]

        for ad in ads_in_break:

            ad_avail_detail.append(media_prep[ad])

            if len(ad_pod_name) < 1:
                ad_pod_name = ad
            else:
                ad_pod_name += "-%s" % (ad)

        media_dict = dict()
        media_dict['house_id'] = ad_pod_name
        media_dict['type'] = "NonProgramEvent"
        media_dict['ad_avail_detail'] = ad_avail_detail
        media_dict['workflow_state'] = {'db_check':'na'}

        if media_dict['house_id'] not in unique_event_list:

            list_for_step_functions_map.append(media_dict)
            unique_event_list.append(media_dict['house_id'])

    event['detail']['clips']['video_workflow'] = list_for_step_functions_map

    event['detail']['list'] = {}
    return event['detail']





    '''
            # we want to filter out the Program/Break Headers
            #return scheduled_event #['PrimaryContent']['Filename']
            house_id = scheduled_event['PrimaryContent']['HouseId']
            total_segments = scheduled_event['PrimaryContent']['TotalSegments'] # int
            s3_uri = "s3://%s" % (scheduled_event['PrimaryContent']['Filename'].replace("\\","/"))
            segment_number = scheduled_event['PrimaryContent']['SegmentNumber']
            
            if house_id not in media_prep.keys():
                media_prep[house_id] = {}
            
            if "segments" not in media_prep[house_id].keys():
                media_prep[house_id]['segments'] = {}
            
            eom_timecode = scheduled_event['PrimaryContent']['Duration']
            som_timecode = scheduled_event['PrimaryContent']['SOM']
            
            # start timecode to ms
            h, m, sms = som_timecode.split(':')
            s = sms.split(frame_separator)[0]
            frames = sms.split(frame_separator)[1][0:2]
            ms = round(int(frames) / frame_rate,3) * 1000
            
            som_seconds = int(h) * 3600 + int(m) * 60 + int(s)
            som_milliseconds = som_seconds * 1000 + int(ms)
            
            # end timecode to ms
            h, m, sms = eom_timecode.split(':')
            s = sms.split(frame_separator)[0]
            frames = sms.split(frame_separator)[1][0:2]
            ms = round(int(frames) / frame_rate,3) * 1000
            
            eom_seconds = int(h) * 3600 + int(m) * 60 + int(s)
            eom_milliseconds = eom_seconds * 1000 + int(ms)
            
            if scheduled_event['EndMode'] == "Duration":
                eom_milliseconds = "end"

                
            media_prep[house_id]['segments'][segment_number] = {"start_ms":som_milliseconds,"end_ms":eom_milliseconds}
            
            
            
            media_prep[house_id]['total_segments'] = total_segments
            media_prep[house_id]['s3'] = s3_uri
            media_prep[house_id]['framerate'] = frame_rate

    for container_uuid in list(versio_list_json['Containers'].keys()):
        container = versio_list_json['Containers'][container_uuid]
        
        if container['ContainerType'] == "Commercial" and container['CType'] == "BreakPod":
            
            container_name = container['Name']
            schedule_uuid_list = container['Children']
            break_pod_list = []
            
            for scheduled_event in versio_list_json['ScheduledEvents']:
                
                content_type = scheduled_event['PrimaryContent']['ContentType']
                uid = scheduled_event['UId']
        
                if uid in schedule_uuid_list and content_type == "Video":
                    # house id and filename
                    house_id = scheduled_event['PrimaryContent']['HouseId']
                    s3_uri = "s3://%s" % (scheduled_event['PrimaryContent']['Filename'].replace("\\","/"))
                    
                    eom_timecode = scheduled_event['PrimaryContent']['Duration']
                    som_timecode = scheduled_event['PrimaryContent']['SOM']
                    
                    # start timecode to ms
                    h, m, sms = som_timecode.split(':')
                    s = sms.split(frame_separator)[0]
                    frames = sms.split(frame_separator)[1][0:2]
                    ms = round(int(frames) / frame_rate,3) * 1000
                    
                    som_seconds = int(h) * 3600 + int(m) * 60 + int(s)
                    som_milliseconds = som_seconds * 1000 + int(ms)
                    
                    # end timecode to ms
                    h, m, sms = eom_timecode.split(':')
                    s = sms.split(frame_separator)[0]
                    frames = sms.split(frame_separator)[1][0:2]
                    ms = round(int(frames) / frame_rate,3) * 1000
                    
                    eom_seconds = int(h) * 3600 + int(m) * 60 + int(s)
                    eom_milliseconds = eom_seconds * 1000 + int(ms)
                    
                    if scheduled_event['EndMode'] == "Duration":
                        eom_milliseconds = "end"
                    
                    break_pod_list.append({"house_id":house_id,"s3":s3_uri,"start_ms":som_milliseconds,"end_ms":eom_milliseconds})
                    
            ad_prep[container_name] = break_pod_list

    event['detail']['clips']['program_content'] = media_prep
    event['detail']['clips']['ad_content'] = ad_prep
    
    
    for media_key in media_prep:
        media_dict = dict()
        media_dict = media_prep[media_key]
        media_dict['house_id'] = media_key
        media_dict['house_id'] = media_key
        media_dict['workflow_state'] = {'db_check':'na'}
        list_for_step_functions_map.append(media_dict)

    for ad_key in ad_prep:
        ad_dict = dict()
        #ad_dict[ad_key] = ad_prep[ad_key]
        ad_dict['break_pod_name'] = ad_key
        ad_dict['pod'] = ad_prep[ad_key]
        media_dict['workflow_state'] = {'db_check':'na'}
        list_for_step_functions_map.append(ad_dict)

    event['detail']['clips']['video_workflow'] = list_for_step_functions_map

    return event['detail']
    '''

    '''    
        house_id = scheduled_event['PrimaryContent']['HouseId'].replace(" ","_").lower()
        content_id = scheduled_event['PrimaryContent']['ContentId']
        
        start_date = scheduled_event['Start']['Date']
        start_date_dt = datetime.strptime(start_date,'%Y-%m-%dT%H:%M:%S')
        start_date_epoch = (start_date_dt - datetime(1970, 1, 1)).total_seconds()
        
        start_timecode = scheduled_event['Start']['Timecode']
        h, m, sms = start_timecode.split(':')
        s = sms.split(frame_separator)[0]
        frames = sms.split(frame_separator)[1][0:2]
        ms = round(int(frames) / frame_rate,3) * 1000
        
        start_timecode = int(h) * 3600 + int(m) * 60 + int(s)
        start_timecode_ms = (start_date_epoch + start_timecode) * 1000 + int(ms)
        
        som_timecode = scheduled_event['SOM']
        h, m, sms = som_timecode.split(':')
        s = sms.split(frame_separator)[0]
        frames = sms.split(frame_separator)[1][0:2]
        ms = round(int(frames) / frame_rate,3) * 1000
        
        som_timecode = (int(h) * 3600 + int(m) * 60 + int(s)) * 1000 + int(ms)
        
        if scheduled_event['EndMode'] == "Duration":
            # this will tell mediaconvert not to put an end clipping timecode
            eom_timecode = 'end'
        else:
            media_duration = scheduled_event['Duration']
    
            h, m, sms = media_duration.split(':')
            s = sms.split(frame_separator)[0]
            frames = sms.split(frame_separator)[1][0:2]
            ms = round(int(frames) / frame_rate,3) * 1000

            eom_timecode = (int(h) * 3600 + int(m) * 60 + int(s)) * 1000 + int(ms)
        
        #time.strftime('%H:%M:%S', time.gmtime(12345))
        # "Timecode": "18:29:57;13%00SD",
        
        
        ca_house_id = "%s_%s_%s" % (house_id,som_timecode,eom_timecode)
    
        
        program = {'ProgramName':ca_house_id,'ChannelName':channel_name,'ScheduledStartTimeMillis':start_timecode_ms,'SourceLocationName':source_location_name,'VodSourceName':''}
        media = {'ca_house_id':ca_house_id,'content_id':content_id,'input_tc':som_timecode,'output_tc':eom_timecode}
        
        program_schedule.append(program)
        media_prep.append(media)
    
    
    
    # Function to get unique assets from the playlist
    unique_programs = []
    for program in media_prep:
        if program not in unique_programs:
            unique_programs.append(program)
        
    return unique_programs
    '''