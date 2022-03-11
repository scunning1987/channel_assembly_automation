import json
import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(event)

    # arguments from API handler
    # return event['detail']['request'] # request uuid
    # return event['detail']['clips'] # empty
    # return event['detail']['workflow_status']

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

    for scheduled_event in versio_list_json['ScheduledEvents']:

        content_type = scheduled_event['PrimaryContent']['ContentType']

        if content_type == "Video":
            # we want to filter out the Program/Break Headers
            #return scheduled_event #['PrimaryContent']['Filename']
            house_id = scheduled_event['PrimaryContent']['HouseId']
            total_segments = scheduled_event['PrimaryContent']['TotalSegments'] # int
            s3_uri = "s3://%s" % (scheduled_event['PrimaryContent']['Filename'].replace("\\","/"))

            if house_id not in media_prep.keys():
                media_prep[house_id] = {}

            if "segments" not in media_prep[house_id].keys():
                media_prep[house_id]['segments'] = []

            media_prep[house_id]['total_segments'] = total_segments
            media_prep[house_id]['s3'] = s3_uri


    return media_prep


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