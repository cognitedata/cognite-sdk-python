import pickle

from cognite.data_transfer_service import DataTransferService


class Model:
    def __init__(self, model):
        self._model = model

    @staticmethod
    def train(file_io, data_spec, **kwargs):
        """The method to train your model.
        Should produce at least a serialized model. Can also produce other
        serialized artefacts that you will need when predicting.

        Args:
            file_io:    A callable which allows you to write to model hosting storage.
                        Works just like the built-in function open
        Keyword Args:
            Any user defined arguments

        Returns:
            None
        """
        my_model = {"input": "result"}

        with file_io("yourmodel.pkl", "wb") as f:
            pickle.dump(my_model, f)
        pass

    def predict(self, instance, data_spec, **kwargs):
        """Method to perform predictions on your model.

        Args:
            instance: The input to your model.
        Keyword Args:
            Any user defined arguments
        Returns:
            Json serializable output from your model.
        """
        return

    @staticmethod
    def load(file_io):
        """Method to load your serialzed model into memory

        Can also load other artifacts such as preprocessors
        Args:
            file_io:    A callable which allows you to write to model hosting storage.
                        Works just like the built-in function open
        Returns:
            An instance of Model
        """
        with file_io("yourmodel.pkl", "rb") as f:
            model = pickle.load(f)
        return Model(model)
