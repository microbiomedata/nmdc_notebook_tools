# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection_search import CollectionSearch
import logging

logger = logging.getLogger(__name__)
# TODO - what are these


class FunctionalAnnotationAggSearch(CollectionSearch):
    """
    Class to interact with the NMDC API to get functional annotation agg sets.
    """

    def __init__(self):
        super().__init__("functional_annotation_agg")
