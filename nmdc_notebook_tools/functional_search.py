# -*- coding: utf-8 -*-

from nmdc_notebook_tools.collection_search import CollectionSearch


class FunctionalSearch:
    """
    Class to interact with the NMDC API to filter functional annotations by KEGG, COG, or PFAM ids.
    """

    def __init__(self):
        self.collectioninstance = CollectionSearch("functional_annotation_agg")

    def get_functional_annotation_id(
        self, id: str, id_type: str, page_size=25, fields="", all_pages=False
    ):
        """
        Get a record from the NMDC API by id. ID types can be KEGG, COG, or PFAM.
        params:
            id: str
                The data base id to query the function annotations.
            id_type:
                The type of id to query. MUST be one of the following:
                    KEGG
                    COG
                    PFAM
            page_size: int
                The number of results to return per page. Default is 25.
            fields: str
                The fields to return. Default is all fields.
                Example: "id,name"
            all_pages: bool
                True to return all pages. False to return the first page. Default is False.
        """
        if id_type not in ["KEGG", "COG", "PFAM"]:
            raise ValueError("id_type must be one of the following: KEGG, COG, PFAM")
        if id_type == "KEGG":
            formatted_id_type = f"KEGG.ORTHOLOGY:{id}"
        elif id_type == "COG":
            formatted_id_type = f"COG:{id}"
        elif id_type == "PFAM":
            formatted_id_type = f"PFAM:{id}"

        filter = f'{{"gene_function_id": "{formatted_id_type}"}}'

        result = self.collectioninstance.get_record_by_filter(
            filter, page_size, fields, all_pages
        )
        return result
