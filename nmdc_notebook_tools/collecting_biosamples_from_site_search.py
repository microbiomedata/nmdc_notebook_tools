# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection_search import CollectionSearch
import logging

logger = logging.getLogger(__name__)


class CollectingBiosamplesFromSiteSearch(CollectionSearch):
    """
    Class to interact with the NMDC API to get collecting biosamples from site sets.
    """

    def __init__(self):
        super().__init__("collecting_biosamples_from_site_set")
