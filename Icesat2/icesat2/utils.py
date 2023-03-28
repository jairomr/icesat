import os
import numpy as np
import pandas as pd

import phoreal as pr
from datetime import datetime



def process_track(file,track):
    atl08 = pr.reader.get_atl08_struct(file,track,epsg = 32721) #wgs84/utm 21S
    atl08.df = atl08.df[atl08.df['cloud_flag_atm']==0] #remove cloudy segments
    atl08.df = atl08.df[atl08.df.latitude > -33.7683777809] #restrict to brazil
    atl08.df = atl08.df[atl08.df.latitude < 5.24448639569]
    ancillary = pr.reader.read_atl09_ancillary_data(file) #read ancillary
    orient = pr.reader.get_attribute_info(file,track)
    start = ancillary.data_start_utc #datetime of file start
    start_time = datetime.strptime(start,"%Y-%m-%dT%H:%M:%S.%fZ") #format
    atl08.df['utc_time'] = start_time #create full column
    atl08.df['gt'] = track #column with ground track
    atl08.df = atl08.df.copy() #copy to modify
    #correct segment start time in utc
    atl08.df['seg_utc_time'] = pd.to_datetime(atl08.df['utc_time']) + pd.to_timedelta(atl08.df['time'], unit='s')
    #use boolean arithmetic to get strong/weak status: if backward left is strong, if forward right is strong
    atl08.df['strength'] = 'strong' if ((orient['sc_orientation']=='backward') + (track[-1]=='l'))!=1 else 'weak'
    #select output
    df_out = atl08.df.iloc[:,[82,89,154,155,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,44,54,41,42,52,60,137,62,63,96,97,105,111,121,128,133,135,136,144,153]].copy()
    return df_out

def process_atl08(file):
    gt_list = ['gt1l','gt1r','gt2l','gt2r','gt3l','gt3r']
    frames = [process_track(file,t) for t in gt_list]
    df_out = pd.concat(frames, ignore_index=True)
    return df_out