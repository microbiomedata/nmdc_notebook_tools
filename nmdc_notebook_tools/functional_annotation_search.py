# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection_search import CollectionSearch
import logging

logger = logging.getLogger(__name__)
# TODO - what are these


class FunctionalAnnotationSearch(CollectionSearch):
    """
    Class to interact with the NMDC API to get functional annotation sets.
    """

    def __init__(self):
        super().__init__("functional_annotation_set")
