import os

from cognite import CogniteClient
from sklearn import linear_model
from sklearn.externals import joblib

# Set the environment variable COGNITE_API_KEY for this to work
client = CogniteClient()

tag_ids = [
    "SKAP_18ESV2113/BCH/10sSamp",
    {"tagId": "SKAP_18PI2101/Y/10sSAMP", "aggregates": ["avg"]},
    {"tagId": "SKAP_18PI2117/Y/10sSAMP", "aggregates": ["avg"]},
]

target_vars = ["SKAP_18PI2117/Y/10sSAMP"]
df = client.datapoints.get_datapoints_frame(tag_ids, aggregates=["step"], granularity="1d", start="50w-ago")

df = df.fillna()

y_labels = [label for label in list(df.columns) if any([label.startswith(var_name) for var_name in target_vars])]
X_labels = [label for label in list(df.columns) if not any([label.startswith(var_name) for var_name in target_vars])]

print()

X = df.drop(["timestamp"] + y_labels, axis=1).values
print(X.shape)
y = df.drop(X_labels, axis=1).values.reshape(X.shape[0])

print(X)
print(y)

# Train a classifier
classifier = linear_model.LinearRegression()
#
classifier.fit(X, y)
print(classifier.predict(X))

# Export the classifier to a file
joblib.dump(classifier, os.path.abspath(os.path.dirname(__file__)) + "/model.joblib")

# Equivalently, you can use the pickle library to export the model similar to:
# import pickle
# with open('model.pkl', 'wb') as model_file:
#   pickle.dump(classifier, model_file)


# The exact file name of of the exported model you upload to GCS is important!
# Your model must be named  model.joblib, model.pkl, or model.bst with respect to
# the library you used to export it.
