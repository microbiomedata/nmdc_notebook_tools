def string_mongo_list(data: list) -> str:
    return str(data).replace("'", '"')


def get_id_list(data: list, id_name: str) -> list:
    return [item[id_name] for item in data]
