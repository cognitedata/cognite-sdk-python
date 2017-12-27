import sys
sys.path.append('../')
from cognite.assets import *
import cognite.config as config

config.configure_session(api_key='3pbGeyjv4hnnzo0SpPmtvCApbaTKAQcv', project='akerbp')

Search_results = searchAssets('xmas').to_pandas()
first = Search_results.ix[0]

sub_assets = getAssets(tagId=first.id).to_pandas()
print(first)
print(sub_assets.to_string())




