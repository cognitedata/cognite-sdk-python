import random
import time

from cognite.client._api.datapoints import DatapointsFetcherNew
from cognite.client.data_classes.datapoints import DatapointsQueryNew

random.seed(42)

queries = [
    DatapointsQueryNew(
        start=random.randint(0, 10),
        end=random.randint(20, 20_000),
        id=i,
    )
    for i in range(10)
]

start = time.perf_counter()
print(f"Executing Datapoints Fetcher for {len(queries):,} queries")
results = DatapointsFetcherNew(None).fetch(queries)
print(f"Retrieved {len(results)} results, time elapsed {time.perf_counter() - start:.2f} seconds")
