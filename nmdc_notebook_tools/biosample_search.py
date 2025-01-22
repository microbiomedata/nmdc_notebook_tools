# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection_search import CollectionSearch
from nmdc_notebook_tools.lat_long_filters import LatLongFilters
import logging

logger = logging.getLogger(__name__)


class BiosampleSearch(LatLongFilters, CollectionSearch):
    """
    Class to interact with the NMDC API to get biosamples.
    """

    def __init__(self):
        super().__init__("biosample_set")
