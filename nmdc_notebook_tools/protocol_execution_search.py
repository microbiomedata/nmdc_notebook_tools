# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection_search import CollectionSearch
import logging

logger = logging.getLogger(__name__)


class ProtocolExecutionSearch(CollectionSearch):
    """
    Class to interact with the NMDC API to get protocol execution sets.
    """

    def __init__(self):
        super().__init__("protocol_execution_set")
