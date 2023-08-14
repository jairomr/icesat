from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, Enum
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from icesat2.config import settings, logger

Base = declarative_base()


ATLGT = Enum('gt1l', 'gt1r', 'gt3r', 'gt3l', 'gt2l', 'gt2r', 
                           name="atl_gt",
                           )

ATLStrength = Enum('weak', 'strong', 
                           name="atl_strength",
                           )


Base = declarative_base()


class Atl8QLRaw(Base):
    __tablename__ = settings.DB_NAME_ATL8
    
    id = Column(Integer, primary_key=True)
    seg_utc_time = Column(DateTime, nullable=True)
    strength = Column(ATLStrength, nullable=True)
    canopy_h_metrics_0 = Column(Float, nullable=True)
    canopy_h_metrics_1 = Column(Float, nullable=True)
    canopy_h_metrics_2 = Column(Float, nullable=True)
    canopy_h_metrics_3 = Column(Float, nullable=True)
    canopy_h_metrics_4 = Column(Float, nullable=True)
    canopy_h_metrics_5 = Column(Float, nullable=True)
    canopy_h_metrics_6 = Column(Float, nullable=True)
    canopy_h_metrics_7 = Column(Float, nullable=True)
    canopy_h_metrics_8 = Column(Float, nullable=True)
    canopy_h_metrics_9 = Column(Float, nullable=True)
    canopy_h_metrics_10 = Column(Float, nullable=True)
    canopy_h_metrics_11 = Column(Float, nullable=True)
    canopy_h_metrics_12 = Column(Float, nullable=True)
    canopy_h_metrics_13 = Column(Float, nullable=True)
    canopy_h_metrics_14 = Column(Float, nullable=True)
    canopy_h_metrics_15 = Column(Float, nullable=True)
    canopy_h_metrics_16 = Column(Float, nullable=True)
    canopy_h_metrics_17 = Column(Float, nullable=True)
    h_canopy = Column(Float, nullable=True)
    h_max_canopy = Column(Float, nullable=True)
    canopy_openness = Column(Float, nullable=True)
    canopy_rh_conf = Column(Integer, nullable=True)
    h_canopy_uncertainty = Column(Float, nullable=True)
    h_min_canopy = Column(Float, nullable=True)
    n_te_photons = Column(Integer, nullable=True)
    n_ca_photons = Column(Integer, nullable=True)
    n_toc_photons = Column(Integer, nullable=True)
    n_seg_ph = Column(Integer, nullable=True)
    night_flag = Column(Boolean, nullable=True)
    segment_landcover = Column(Integer, nullable=True)
    sigma_h = Column(Float, nullable=True)
    h_te_best_fit = Column(Float, nullable=True)
    h_te_max = Column(Float, nullable=True)
    h_te_rh25 = Column(Float, nullable=True)
    h_te_std = Column(Float, nullable=True)
    h_te_uncertainty = Column(Float, nullable=True)
    terrain_slope = Column(Float, nullable=True)
    gt = Column(String, nullable=True)
    sigma = Column(Float, nullable=True)
    pop_mean = Column(Float, nullable=True)
    geometry = Column(Geometry(geometry_type='POINT', srid=4326), nullable=True)
    _id = Column(Integer, nullable=True)
    geohash = Column(String(length=3), nullable=True)
    
    
