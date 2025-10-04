def create_handler():
    def inner_function(data, client, secrets):
        return {"assetId": 1234}

    return inner_function


handle = create_handler()
