from geoalchemy2 import Geometry
from icesat2.config import logger, settings
from sqlalchemy import (Boolean, Column, DateTime, Enum, Float, Integer,
                        SmallInteger, String)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


ATLGT = Enum(
    'gt1l',
    'gt1r',
    'gt3r',
    'gt3l',
    'gt2l',
    'gt2r',
    name='atl_gt',
)

ATLStrength = Enum(
    'weak',
    'strong',
    name='atl_strength',
)


Base = declarative_base()


class Atl8Raw(Base):
    __tablename__ = settings.DB_NAME_ATL8

    id = Column(Integer, primary_key=True)
    _id = Column(Integer, nullable=True)
    seg_utc_time = Column(DateTime, nullable=True)
    strength = Column(ATLStrength, nullable=True)
    canopy_h_metrics_0 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_1 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_2 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_3 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_4 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_5 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_6 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_7 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_8 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_9 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_10 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_11 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_12 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_13 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_14 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_15 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_16 = Column(Float(precision=32), nullable=True)
    canopy_h_metrics_17 = Column(Float(precision=32), nullable=True)
    h_canopy = Column(Float(precision=32), nullable=True)
    h_max_canopy = Column(Float(precision=32), nullable=True)
    canopy_openness = Column(Float(precision=32), nullable=True)
    canopy_rh_conf = Column(SmallInteger, nullable=True)
    h_canopy_uncertainty = Column(Float(precision=32), nullable=True)
    h_min_canopy = Column(Float(precision=32), nullable=True)
    n_te_photons = Column(SmallInteger, nullable=True)
    n_ca_photons = Column(SmallInteger, nullable=True)
    n_toc_photons = Column(SmallInteger, nullable=True)
    n_seg_ph = Column(SmallInteger, nullable=True)
    night_flag = Column(Boolean, nullable=True)
    segment_landcover = Column(SmallInteger, nullable=True)
    sigma_h = Column(Float(precision=32), nullable=True)
    h_te_best_fit = Column(Float(precision=32), nullable=True)
    h_te_max = Column(Float(precision=32), nullable=True)
    h_te_rh25 = Column(Float(precision=32), nullable=True)
    h_te_std = Column(Float(precision=32), nullable=True)
    h_te_uncertainty = Column(Float(precision=32), nullable=True)
    terrain_slope = Column(Float(precision=32), nullable=True)
    gt = Column(ATLGT, nullable=True)
    sigma = Column(Float(precision=32), nullable=True)
    pop_mean = Column(Float(precision=32), nullable=True)
    geometry = Column(
        Geometry(geometry_type='POINT', srid=4326), nullable=True, index=False
    )
    
    geohash = Column(String(length=5), nullable=True)


class Atl3Raw(Base):
    __tablename__ = settings.DB_NAME_ATL3
    
    """
    seg_id                     int32
    ph_utc_time       datetime64[ns]
    norm_h                   float64
    h_ph                     float32
    classification             int64
    night                       bool
    quality_ph                  int8
    signal_conf_ph              int8
    beam                       int64
    
    geohash                   object
    geometry                geometry
    _id                        int64
    """
    id = Column(Integer, primary_key=True)
    _id = Column(Integer, nullable=True)
    seg_id = Column(Integer,nullable=True)
    ph_utc_time = Column(DateTime, nullable=True)
    norm_h = Column(Float(precision=32), nullable=True)
    h_ph = Column(Float(precision=32), nullable=True)
    classification = Column(SmallInteger, nullable=True)
    night = Column(Boolean, nullable=True)
    quality_ph = Column(SmallInteger, nullable=True)
    signal_conf_ph = Column(SmallInteger, nullable=True)
    beam = Column(Boolean, nullable=True)
    geometry = Column(
        Geometry(geometry_type='POINT', srid=4326), nullable=True, index=False
    )
    
    geohash = Column(String(length=5), nullable=True)
