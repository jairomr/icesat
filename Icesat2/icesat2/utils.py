import os
from datetime import datetime
from icesat2.config import logger

import numpy as np
import pandas as pd
import phoreal as pr


def process_track(file, track):
    try:
        atl08 = pr.reader.get_atl08_struct(
            file, track, epsg=32721
        )   # wgs84/utm 21S
        
        
        # Filtros para selecionar os dados desejados
        filter_cloudy = atl08.df['cloud_flag_atm'] == 0
        filter_latitude = (atl08.df['latitude'] > -33.7683777809) & (atl08.df['latitude'] < 5.24448639569)
        filter_canopy_openness = atl08.df['canopy_openness'] <= 100
        filter_h_te_std = atl08.df['h_te_std'] <= 100

        # Aplicar os filtros em sequência
        atl08.df = atl08.df[filter_cloudy & filter_latitude & filter_canopy_openness & filter_h_te_std]
        
        
        ancillary = pr.reader.read_atl09_ancillary_data(file)   # read ancillary
        orient = pr.reader.get_attribute_info(file, track)
        start = ancillary.data_start_utc   # datetime of file start
        start_time = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S.%fZ')   # format
        atl08.df['utc_time'] = start_time   # create full column
        atl08.df['gt'] = track   # column with ground track
        atl08.df = atl08.df.copy()   # copy to modify
        # correct segment start time in utc
        atl08.df['seg_utc_time'] = pd.to_datetime(
            atl08.df['utc_time']
        ) + pd.to_timedelta(atl08.df['time'], unit='s')
        # use boolean arithmetic to get strong/weak status: if backward left is strong, if forward right is strong
        atl08.df['strength'] = (
            'strong'
            if ((orient['sc_orientation'] == 'backward') + (track[-1] == 'l')) != 1
            else 'weak'
        )
        # calculate overall sigma
        atl08.df['sigma'] = np.sqrt(
            (
                (
                    (
                        ((atl08.df.n_seg_ph - atl08.df.n_te_photons) - 1)
                        * np.square(atl08.df.canopy_openness)
                    )
                    + ((atl08.df.n_te_photons - 1) * np.square(atl08.df.h_te_std))
                )
                / (atl08.df.n_seg_ph - 1)
            )
            + (
                (atl08.df.n_seg_ph - atl08.df.n_te_photons)
                * atl08.df.n_te_photons
                * np.square(atl08.df.h_mean_canopy_abs - atl08.df.h_te_mean)
            )
            / ((atl08.df.n_seg_ph) * (atl08.df.n_seg_ph - 1))
        )
        atl08.df['pop_mean'] = (
            (atl08.df.n_seg_ph - atl08.df.n_te_photons)
            * atl08.df.h_mean_canopy_abs
            + atl08.df.n_te_photons * atl08.df.h_te_mean
        ) / (atl08.df.n_seg_ph) - (
            atl08.df.h_mean_canopy_abs - atl08.df.h_mean_canopy
        )
        # select output
        df_out = atl08.df.loc[
            :,
            [
                'latitude',
                'longitude',
                'seg_utc_time',
                'strength',
                'canopy_h_metrics_0',
                'canopy_h_metrics_1',
                'canopy_h_metrics_2',
                'canopy_h_metrics_3',
                'canopy_h_metrics_4',
                'canopy_h_metrics_5',
                'canopy_h_metrics_6',
                'canopy_h_metrics_7',
                'canopy_h_metrics_8',
                'canopy_h_metrics_9',
                'canopy_h_metrics_10',
                'canopy_h_metrics_11',
                'canopy_h_metrics_12',
                'canopy_h_metrics_13',
                'canopy_h_metrics_14',
                'canopy_h_metrics_15',
                'canopy_h_metrics_16',
                'canopy_h_metrics_17',
                'h_canopy',
                'h_max_canopy',
                'canopy_openness',
                'canopy_rh_conf',
                'h_canopy_uncertainty',
                'h_min_canopy',
                'n_te_photons',
                'n_ca_photons',
                'n_toc_photons',
                'n_seg_ph',
                'night_flag',
                'segment_landcover',
                'sigma_h',
                'h_te_best_fit',
                'h_te_max',
                'h_te_rh25',
                'h_te_std',
                'h_te_uncertainty',
                'terrain_slope',
                'gt',
                'sigma',
                'pop_mean',
            ],
        ].copy()
        return df_out
    except KeyError:
        logger.exception('Error com coluna faltando')
        return pd.DataFrame()


def process_atl08(file):
    gt_list = ['gt1l', 'gt1r', 'gt2l', 'gt2r', 'gt3l', 'gt3r']
    frames = [process_track(file, t) for t in gt_list]
    df_out = pd.concat(frames, ignore_index=True)
    return df_out


def process03_track(atl03_file, atl08_file, track):
    atl03 = pr.reader.get_atl03_struct(
        atl03_file, track, atl08_file, epsg=32721
    )   # wgs84/utm 21S
    geolocation = pr.reader.read_atl03_geolocation(atl03_file, track)
    heights = atl03.df
    heights = pr.reader.append_atl03_geolocation(
        heights, geolocation, fields=['solar_azimuth']
    )   # add azimuth
    heights = heights[atl03.df.lat_ph > -33.7683777809]   # separate Brazil
    heights = heights[atl03.df.lat_ph < 5.24448639569]
    heights.columns = [
        'delta_time',
        'dist_ph_across',
        'dist_ph_along',
        'h_ph',
        'lat_ph',
        'lon_ph',
        'pce_mframe_cnt',
        'ph_id_channel',
        'ph_id_count',
        'ph_id_pulse',
        'quality_ph',
        'signal_conf_ph',
        'classification',
        'norm_h',
        'seg_id',
        'ph_bihr',
        'ph_bcr',
        'ph_rate',
        'time',
        'utc_time',
        'easting',
        'northing',
        'crosstrack',
        'alongtrack',
        'solar_azimuth',
    ]   # fix naming (two time columns)
    heights['time_add'] = pd.to_timedelta(heights['time'], unit='s')
    start_time = datetime(
        **{
            'year': int(atl03.year),
            'month': int(atl03.month),
            'day': int(atl03.day),
            'hour': int(atl03.hour),
            'minute': int(atl03.minute),
            'second': int(atl03.second),
        }
    )
    heights = heights.copy()   # need a copy to modify utc_time
    heights.utc_time = (
        start_time  # copy start time to full column for modification
    )
    heights['ph_utc_time'] = (
        pd.to_datetime(heights.utc_time) + heights['time_add']
    )   # get individual photon time
    heights['beam'] = 1 if atl03.beamStrength == 'strong' else 0
    heights['night'] = heights['solar_azimuth'] < 0   # binary day=0 / night=1
    heights = heights[
        heights.classification > 0
    ]   # remove photons not classified as signal
    df_out = heights[
        [
            'seg_id',
            'ph_utc_time',
            'lat_ph',
            'lon_ph',
            'norm_h',
            'h_ph',
            'classification',
            'night',
            'quality_ph',
            'signal_conf_ph',
            'beam',
        ]
    ].copy()
    return df_out


def process_atl03(atl03_file, atl08_file):
    gt_list = ['gt1l', 'gt1r', 'gt2l', 'gt2r', 'gt3l', 'gt3r']
    frames = [process03_track(atl03_file, atl08_file, t) for t in gt_list]
    df_out = pd.concat(frames, ignore_index=True)
    return df_out
