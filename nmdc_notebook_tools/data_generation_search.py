# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection_search import CollectionSearch
import logging

logger = logging.getLogger(__name__)


class DataGenerationSearch(CollectionSearch):
    """
    Class to interact with the NMDC API to get data generation sets.
    """

    def __init__(self):
        super().__init__("data_generation_set")
