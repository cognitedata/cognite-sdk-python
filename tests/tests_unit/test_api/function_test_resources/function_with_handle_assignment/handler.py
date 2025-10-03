def create_handler():
    def handle(data, client, secrets):
        return {"assetId": 1234}

    return handle


handle = create_handler()
