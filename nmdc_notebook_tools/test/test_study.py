# -*- coding: utf-8 -*-
from nmdc_notebook_tools.study_search import StudySearch


def test_find_study_by_attribute():
    st = StudySearch()
    stu = st.get_record_by_attribute(
        "name",
        "Lab enrichment of tropical soil microbial communities from Luquillo Experimental Forest, Puerto Rico",
    )
    assert len(stu) > 0


def test_find_study_by_filter():
    st = StudySearch()
    stu = st.get_record_by_filter(
        '{"name":"Lab enrichment of tropical soil microbial communities from Luquillo Experimental Forest, Puerto Rico"}'
    )
    assert len(stu) > 0
    assert (
        stu[0]["name"]
        == "Lab enrichment of tropical soil microbial communities from Luquillo Experimental Forest, Puerto Rico"
    )
