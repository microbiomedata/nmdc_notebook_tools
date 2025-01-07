# -*- coding: utf-8 -*-
from nmdc_notebook_tools.collection import Collection


def test_get_collection():
    # testing the filters
    collection = Collection()
    results = collection.get_collection("study_set")
    assert len(results) > 0


def test_get_collection_data_object_by_type():
    collection = Collection()
    results = collection.get_collection_data_object_by_type(
        "data_object_set", "nmdc:bsm-11-002vgm56"
    )
    assert len(results) > 0
