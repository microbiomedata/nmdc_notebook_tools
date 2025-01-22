# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection_search import CollectionSearch
import logging

logger = logging.getLogger(__name__)


class MaterialProcessingSearch(CollectionSearch):
    """
    Class to interact with the NMDC API to get material processing sets.
    """

    def __init__(self):
        super().__init__("material_processing_set")
