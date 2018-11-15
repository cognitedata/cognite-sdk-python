import pickle
from sklearn.ensemble import RandomForestRegressor
from cognite.data_transfer_service import DataTransferService

class Model:
    """
    You need to have a class called Model in a file called model.py at the 
    top level of your package.

    It should contain
        - Static train method
            Which performs training and persist any state you need for
            prediction. This can be serialized models, csv, or something else.
            You just have to be able to store it in files.
        - Static load method
            Which load your persisted state and return an instance of the Model
            class that are ready for predictions.
        - Predict method
            Which use the persisted state to do predictions.

    """
    @staticmethod
    def train(file_io, data_spec, api_key, project, **kwargs):
        """
        file_io:
            The train method must accept a file_io argument. This is a function
            that works the same way as the builtin open(), except it reads from
            and writes to the root of a special storage location in the cloud
            that belongs to the current model version.
        data_spec:
            An argument we pass in ourself when we initiate the training.
        api_key, project:
            Optional arguments that are passed in automatically from Model
            Hosting for your convenience. The API key is the one that were
            used to initiate this training routine through the Model Hosting
            HTTP API.
        """
        dts = DataTransferService(data_spec, api_key=api_key, project=project)
        df = dts.get_dataframe().dropna()

        X = df[["temp", "pressure", "rpm"]].values
        y = df["production_rate"].values

        regressor = RandomForestRegressor(n_estimators=10, min_samples_split=100) # We'll mostly use default settings
        regressor.fit(X, y)

        # Persist our regressor model
        with file_io("regressor.pickle", "wb") as f:
            pickle.dump(regressor, f)

    def __init__(self, regressor):
        self.regressor = regressor

    @staticmethod
    def load(file_io):
        """
        We'll use file_io to access and load the regressor we fitted during
        training. We then return an instance of the Model class that are ready
        for doing predictions.
        """
        with file_io("regressor.pickle", "rb") as f:
            regressor = pickle.load(f)
        return Model(regressor)
    
    def predict(self, instance, api_key, project, **kwargs):
        """
        instance:
            Since we're doing scheduled prediction, this will be a data spec
            describing the data we should do prediction on.
        
        Note that it's also possible to take api_key and project in as
        optional arguments here the same way as in train().
        """
        dts = DataTransferService(instance, api_key=api_key, project=project)
        df = dts.get_dataframe().dropna()

        X = df[["temp", "pressure", "rpm"]].values
        df["production_rate"] = self.regressor.predict(X)

        # For scheduled prediction we need to return output on the format:
        # {
        #   "timestamp": [t0, t1, t2, ...],
        #   "production_rate": [p0, p1, p2, ...]
        # }
        # And we can call to_dict(orient="list") on our pandas DataFrame to get
        # our prediction on that format.
        return df[["timestamp", "production_rate"]].to_dict(orient="list")
