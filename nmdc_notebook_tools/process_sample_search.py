# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection_search import CollectionSearch
import logging

logger = logging.getLogger(__name__)


class ProcessSampleSearch(CollectionSearch):
    """
    Class to interact with the NMDC API to get process sample sets.
    """

    def __init__(self):
        super().__init__("process_sample_set")
