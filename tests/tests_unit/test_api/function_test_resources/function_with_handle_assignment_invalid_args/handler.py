def create_handler():
    def inner_function(data, client, secrets, invalid_arg, another_invalid):
        return {"assetId": 1234}

    return inner_function


handle = create_handler()
