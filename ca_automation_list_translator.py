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

    # arguments from API handler
    # return event['detail']['request'] # request uuid
    # return event['detail']['clips'] # empty
    # return event['detail']['workflow_status']

    def hms_to_s(input_hms):
        h, m, sms = input_hms.split(':')
        s = sms.split(frame_separator)[0]
        frames = sms.split(frame_separator)[1][0:2]
        total_ms = round(int(frames) / frame_rate,3) * 1000

        total_seconds = int(h) * 3600 + int(m) * 60 + int(s)

        return {"s":total_seconds,"ms":total_ms}


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



    # Get request payload from S3 and put into event key
    s3bucket = event['list_location'].split("/")[2]
    s3key = '/'.join(event['list_location'].split("/")[3:])
    request_list = json.loads(get_req_data(s3bucket,s3key))
    if len(exceptions) > 0:
        raise Exception(exceptions)
    event['list'] = request_list


    channel_name = event['list']['PlayoutChannel']

    versio_list_json = event['list']

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

    scheduled_events = versio_list_json['ScheduledEvents']
    schedule_containers = versio_list_json['Containers']


    child_schedule_item = []
    break_index = 1
    for schedule_number in range(0,len(scheduled_events)):
        LOGGER.info("Iterating through scheduled event : %s " % (str(schedule_number)))

        if schedule_number not in child_schedule_item:



            child_schedule_item.append(schedule_number)

            schedule_item = scheduled_events[schedule_number]

            title = schedule_item['Title']
            content_type = schedule_item['PrimaryContent']['ContentType']
            total_segments = schedule_item['PrimaryContent']['TotalSegments'] # int
            segment_number = schedule_item['PrimaryContent']['SegmentNumber']
            item_type = schedule_item['Type'] # ProgramEvent = program , NonProgramEvent = ad

            # if schedule_parent_id not in list(schedule_containers.keys()):
            #     container_type = "other"
            #     LOGGER.info("schedule item %s does not have a parent id in containers" % (title))
            # else:
            #     container_type = schedule_containers[schedule_parent_id]['ContainerType']

            if content_type == "Video":

                house_id = schedule_item['PrimaryContent']['HouseId']
                start_date_iso = schedule_item['Start']['Date']
                start_time_hms = schedule_item['Start']['Timecode']

                duration_hms = schedule_item['Duration']

                duration_s_and_ms = hms_to_s(duration_hms)

                parsed_start_date = dp.parse(start_date_iso)
                start_date_epoch = int(parsed_start_date.strftime('%s'))

                start_s_and_ms = hms_to_s(start_time_hms)

                start_epochms = (start_date_epoch + start_s_and_ms['s']) * 1000 + int(start_s_and_ms['ms'])

                # program_name = "%s_%s" % (house_id,str(start_epochms))
                program_name = house_id


                program_entry = {"ChannelName":channel_name,"ProgramName":program_name,"ScheduleConfiguration":{"Transition":{"RelativePosition":"BEFORE_PROGRAM","ScheduledStartTimeMillis":start_epochms,"Type":"ABSOLUTE"}},"AdBreaks":[]}
                adbreak_template = {"MessageType":"SPLICE_INSERT","OffsetMillis":0,"Slate":{"SourceLocationName":"tbd","VodSourceName":"ad_house_id"},"SpliceInsertMessage":{"AvailNum":break_index,"AvailsExpected":2,"SpliceEventId":break_index,"UniqueProgramId":break_index}}

                if total_segments == 0: # single segment program or ad slot

                    if item_type == "ProgramEvent":
                        LOGGER.info("this is a program: %s " % (title))

                        program_schedule.append(program_entry)
                        LOGGER.info("Scheduled item %s is a Primary Program, Title: %s" % (str(schedule_number),title))
                        # This is the primary programming event

                        # now check subsequent schedule items to see if there is an ad pod after this item
                        # child_schedule_item.append(x)
                        avails = 0
                        break_house_id = ""
                        lookahead_threshold = schedule_number + 2


                        for schedule_number_adcheck in range(schedule_number+1,len(scheduled_events)):


                            schedule_item_child = scheduled_events[schedule_number_adcheck]

                            title = schedule_item_child['Title']
                            content_type = schedule_item_child['PrimaryContent']['ContentType']
                            total_segments = schedule_item_child['PrimaryContent']['TotalSegments'] # int
                            item_type = schedule_item_child['Type'] # ProgramEvent = program , NonProgramEvent = ad




                            if item_type != "ProgramEvent":

                                if content_type == "Video":

                                    child_house_id = schedule_item_child['PrimaryContent']['HouseId']

                                    if item_type == "NonProgramEvent": # This is adbreak
                                        avails += 1
                                        if len(break_house_id) < 1:
                                            break_house_id = child_house_id
                                        else:
                                            break_house_id += "-" + child_house_id

                                        child_schedule_item.append(schedule_number_adcheck)

                                    elif child_house_id != house_id:



                                        break

                            else:


                                break

                        # create the ad break in the schedule json
                        adbreak_template['OffsetMillis'] = (duration_s_and_ms['s']*1000)+duration_s_and_ms['ms']
                        adbreak_template['Slate']['VodSourceName'] = break_house_id
                        program_entry['AdBreaks'].append(adbreak_template)
                        break_index += 1

                else:
                    # multi-segment madness! This will translate to a single program with multiple breaks

                    if item_type == "ProgramEvent":
                        LOGGER.info("this is a multi-segment program: %s " % (title))

                        if segment_number == 1:
                            #program_name = "%s_%s" % (house_id,str(start_epochms))

                            program_schedule.append(program_entry)

                        LOGGER.info("Scheduled item %s is a Primary Program, Title: %s" % (str(schedule_number),title))
                        # This is the primary programming event

                        # now check subsequent schedule items to see if there is an ad pod after this item
                        # child_schedule_item.append(x)
                        avails = 0
                        break_house_id = ""
                        lookahead_threshold = schedule_number + 2


                        iterated_items = []
                        adbreaks = []
                        skip = 0
                        offset_ms = (duration_s_and_ms['s']*1000)+duration_s_and_ms['ms']

                        for schedule_number_adcheck in range(schedule_number+1,len(scheduled_events)):

                            child_schedule_item.append(schedule_number_adcheck)


                            schedule_item_child = scheduled_events[schedule_number_adcheck]

                            child_title = schedule_item_child['Title']
                            content_type = schedule_item_child['PrimaryContent']['ContentType']
                            child_total_segments = schedule_item_child['PrimaryContent']['TotalSegments'] # int
                            child_segment_number = schedule_item_child['PrimaryContent']['SegmentNumber']
                            item_type = schedule_item_child['Type'] # ProgramEvent = program , NonProgramEvent = ad
                            child_house_id = schedule_item_child['PrimaryContent']['HouseId']

                            if content_type == "Video":

                                if item_type == "NonProgramEvent": # This is adbreak

                                    LOGGER.info("Primary event: %s, currently looking at event %s, it is an ad" % (title,child_title))
                                    avails += 1
                                    if len(break_house_id) < 1:
                                        break_house_id = child_house_id
                                    else:
                                        break_house_id += "-" + child_house_id

                                    child_schedule_item.append(schedule_number_adcheck)

                                if item_type == "ProgramEvent" and child_house_id == house_id: # This is the next segment

                                    if child_segment_number is not segment_number:

                                        if avails > 0:
                                            adbreak_template = {"MessageType":"SPLICE_INSERT","OffsetMillis":0,"Slate":{"SourceLocationName":"tbd","VodSourceName":"ad_house_id"},"SpliceInsertMessage":{"AvailNum":break_index,"AvailsExpected":avails,"SpliceEventId":break_index,"UniqueProgramId":break_index}}

                                            adbreak_template['OffsetMillis'] = offset_ms # (duration_s_and_ms['s']*1000)+duration_s_and_ms['ms']
                                            adbreak_template['Slate']['VodSourceName'] = break_house_id
                                            adbreaks.append(adbreak_template)

                                        break_house_id = ""
                                        break_index += 1
                                        avails = 0

                                        # We are going to assume the list was created with multi-segment content and they are incrementing correctly

                                        duration_hms = schedule_item_child['Duration']
                                        child_duration_s_and_ms = hms_to_s(duration_hms)

                                        # increment time offset

                                        offset_ms += (child_duration_s_and_ms['s']*1000) + child_duration_s_and_ms['ms']


                                    else: # new program


                                        if avails > 0:
                                            adbreak_template = {"MessageType":"SPLICE_INSERT","OffsetMillis":0,"Slate":{"SourceLocationName":"tbd","VodSourceName":"ad_house_id"},"SpliceInsertMessage":{"AvailNum":break_index,"AvailsExpected":avails,"SpliceEventId":break_index,"UniqueProgramId":break_index}}

                                            adbreak_template['OffsetMillis'] = offset_ms # (duration_s_and_ms['s']*1000)+duration_s_and_ms['ms']
                                            adbreak_template['Slate']['VodSourceName'] = break_house_id
                                            adbreaks.append(adbreak_template)

                                        # This is a new program
                                        child_schedule_item.pop(-1)


                                        break_house_id = ""
                                        avails = 0
                                        break_index += 1

                                        LOGGER.info("Primary event: %s, currently looking at event %s, it seems to be a subsequent segment of the initial primary event" % (title,child_title))

                                        #iterated_items.append(title)

                                        # We are going to assume the list was created with multi-segment content and they are incrementing correctly

                                        duration_hms = schedule_item_child['Duration']
                                        child_duration_s_and_ms = hms_to_s(duration_hms)

                                        # increment time offset
                                        offset_ms += (child_duration_s_and_ms['s']*1000) + child_duration_s_and_ms['ms']


                                        if child_segment_number == 1:
                                            child_schedule_item.pop(-1)
                                            break





                            ###
                            # if item_type != "ProgramEvent": # Looking for a possible ad break

                            #     LOGGER.info("Primary event: %s, currently looking at event %s, its not a primary event, checking to see if its an ad" % (title,child_title))

                            #     if content_type == "Video":

                            #         if item_type == "NonProgramEvent": # This is adbreak

                            #             LOGGER.info("Primary event: %s, currently looking at event %s, it is an ad" % (title,child_title))
                            #             avails += 1
                            #             if len(break_house_id) < 1:
                            #                 break_house_id = child_house_id
                            #             else:
                            #                 break_house_id += "-" + child_house_id

                            #             child_schedule_item.append(schedule_number_adcheck)



                            #         else:

                            #             break

                            # elif item_type == "ProgramEvent" and child_house_id == house_id: # This is the next segment

                            #     iterated_items.append(break_house_id)

                            #     if child_segment_number != segment_number: # There may be an occurrence in a list of same segment repeating. We should treat that as separate program

                            #         if avails > 0:
                            #             adbreak_template = {"MessageType":"SPLICE_INSERT","OffsetMillis":0,"Slate":{"SourceLocationName":"tbd","VodSourceName":"ad_house_id"},"SpliceInsertMessage":{"AvailNum":break_index,"AvailsExpected":1,"SpliceEventId":break_index,"UniqueProgramId":break_index}}

                            #             adbreak_template['OffsetMillis'] = offset_ms # (duration_s_and_ms['s']*1000)+duration_s_and_ms['ms']
                            #             adbreak_template['Slate']['VodSourceName'] = break_house_id
                            #             adbreaks.append(adbreak_template)

                            #     else:

                            #         # This is a new program
                            #         child_schedule_item.pop(-1)


                            #         break_house_id = ""
                            #         avails = 0
                            #         break_index += 1

                            #         LOGGER.info("Primary event: %s, currently looking at event %s, it seems to be a subsequent segment of the initial primary event" % (title,child_title))

                            #         #iterated_items.append(title)

                            #         # We are going to assume the list was created with multi-segment content and they are incrementing correctly

                            #         duration_hms = schedule_item_child['Duration']
                            #         child_duration_s_and_ms = hms_to_s(duration_hms)

                            #         # increment time offset
                            #         offset_ms += (child_duration_s_and_ms['s']*1000) + child_duration_s_and_ms['ms']

                            #         if child_segment_number == 1:
                            #             child_schedule_item.pop(-1)
                            #             break

                            #     else:
                            #         LOGGER.warning("The list contains back to back Programs that are the same segment number. treating as different program")
                            #         child_schedule_item.pop(-1)

                            # else:

                            #     LOGGER.info("Primary event is: %s, currently looking at event %s, it does not seem to be an ad or primary event. exiting the iteration through the schedule" % (title,child_title))

                            #     break

                            ###


                        # return adbreaks
                        program_entry['AdBreaks'] = adbreaks
                        # return program_entry
                        # create the ad break in the schedule json
                        # adbreak_template['OffsetMillis'] = (duration_s_and_ms['s']*1000)+duration_s_and_ms['ms']
                        # adbreak_template['Slate']['VodSourceName'] = break_house_id
                        # program_entry['AdBreaks'].append(adbreak_template)
        else:
            LOGGER.info("Already looked at this event as a child event")

    event['channel_assembly_schedule'] = program_schedule
    event['list'] = {}
    return event

    '''


    for scheduled_event in versio_list_json['ScheduledEvents']:
        
        content_type = scheduled_event['PrimaryContent']['ContentType']
        
        if content_type == "Video":
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

    event['clips']['program_content'] = media_prep
    event['clips']['ad_content'] = ad_prep
    
    
    for media_key in media_prep:
        media_dict = dict()
        media_dict = media_prep[media_key]
        media_dict['house_id'] = media_key
        list_for_step_functions_map.append(media_dict)

    for ad_key in ad_prep:
        ad_dict = dict()
        #ad_dict[ad_key] = ad_prep[ad_key]
        ad_dict['break_pod_name'] = ad_key
        ad_dict['pod'] = ad_prep[ad_key]
        list_for_step_functions_map.append(ad_dict)

    event['clips']['video_workflow'] = list_for_step_functions_map

    return event
        
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