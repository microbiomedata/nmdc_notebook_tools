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
