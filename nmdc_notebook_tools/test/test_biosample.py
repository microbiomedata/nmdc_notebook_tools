# -*- coding: utf-8 -*-
from nmdc_notebook_tools.biosample import Biosample
import logging


def test_find_biosample_by_id():
    biosample = Biosample()
    results = biosample.find_biosample_by_id("nmdc:bsm-11-002vgm56")
    assert len(results) > 0
    assert results["id"] == "nmdc:bsm-11-002vgm56"


def test_logger():
    biosample = Biosample()
    logging.basicConfig(level=logging.DEBUG)
    results = biosample.find_biosample_by_id("nmdc:bsm-11-002vgm56")


def test_biosample_by_filter():
    biosample = Biosample()
    results = biosample.biosample_by_filter('{"id":"nmdc:bsm-11-006pnx90"}')
    assert len(results) > 0


def test_biosample_by_attribute():
    biosample = Biosample()
    results = biosample.biosample_by_attribute("id", "nmdc:bsm-11-006pnx90")
    assert len(results) > 0


def test_biosample_by_latitude():
    # {"lat_lon.latitude": {"$gt": 45.0}, "lat_lon.longitude": {"$lt":45}}
    biosample = Biosample()
    results = biosample.biosample_by_latitude("gt", 45.0)
    assert len(results) > 0
    assert results[0]["lat_lon"]["latitude"] == 63.875088


def test_biosample_by_longitude():
    # {"lat_lon.latitude": {"$gt": 45.0}, "lat_lon.longitude": {"$lt":45}}
    biosample = Biosample()
    results = biosample.biosample_by_longitude("lt", 45.0)
    assert len(results) > 0
    assert results[0]["lat_lon"]["longitude"] == -149.210438


def test_biosample_by_lat_long():
    # {"lat_lon.latitude": {"$gt": 45.0}, "lat_lon.longitude": {"$lt":45}}
    biosample = Biosample()
    results = biosample.biosample_by_lat_long("gt", "lt", 45.0, 45.0)
    assert len(results) > 0
    assert results[0]["lat_lon"]["latitude"] == 63.875088
    assert results[0]["lat_lon"]["longitude"] == -149.210438
