# -*- coding: utf-8 -*-
def string_mongo_list(data: list) -> str:
    """
    Convert elements in a list to use double quotes instead of single quotes.
    This is required for mongo queries.
    """
    return str(data).replace("'", '"')


def split_list(data: list, chunk_size: int) -> list:
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def get_id_list(data: list, id_name: str) -> list:
    """
    Get a list of ids from an api call response json.

    """
    return [item[id_name] for item in data]
