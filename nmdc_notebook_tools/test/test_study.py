# -*- coding: utf-8 -*-
from nmdc_notebook_tools.study import Study


def test_find_study_by_attribute():
    st = Study()
    stu = st.find_study_by_attribute(
        "name",
        "Deep subsurface shale carbon reservoir microbial communities from Ohio and West Virginia",
    )


def test_find_study_by_filter():
    st = Study()
    stu = st.find_study_by_filter(
        '{"name":"Deep subsurface shale carbon reservoir microbial communities from Ohio and West Virginia"}'
    )
