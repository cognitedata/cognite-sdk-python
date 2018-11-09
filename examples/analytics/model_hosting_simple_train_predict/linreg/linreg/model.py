import pandas as pd
import numpy as np
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
            and writes to a special Model Hosting location in the cloud.
        data_spec:
            An argument we pass in ourself when we initiate the training.
        api_key, project:
            Optional arguments that are passed in automatically from Model
            Hosting for your convenience.
        """
        dts = DataTransferService(data_spec, api_key=api_key, project=project)
        X = pd.read_csv(dts.get_file("data"))
        y = pd.read_csv(dts.get_file("target"))

        # Add a feature of constant value 1
        X.insert(0, "f0", 1)

        # Least squares
        coefficients = pd.DataFrame(
            np.linalg.inv(X.T.dot(X)).dot(X.T).dot(y),
            columns=["beta_hat"]
        )

        # Persist our result
        with file_io("coefficients.csv", "w") as f:
            coefficients.to_csv(f, index=False)

    def __init__(self, coefficients):
        self.coefficients = coefficients

    @staticmethod
    def load(file_io):
        """
        We'll use file_io to access and load the coefficients we found during
        training. We then return an instance of the Model class that are ready
        for doing predictions.
        """
        with file_io("coefficients.csv", "r") as f:
            coefficients = pd.read_csv(f)
        return Model(coefficients)
    
    def predict(self, instance, precision=2, **kwargs):
        """
        instance:
            The value we want to do prediction on. In our case this is a list
            of two numbers.
        precision:
            Optional argument we have defined ourselves.
        """
        instance = pd.DataFrame([[1] + instance], columns=["f0", "f1", "f2"])
        prediction = float(np.dot(instance, self.coefficients))
        prediction = round(prediction, precision)
        return prediction