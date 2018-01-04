import sys
sys.path.append('../')
from cognite.assets import *
import cognite.config as config

key = sys.argv[1]
config.configure_session(api_key=key, project='akerbp')

Search_results = searchAssets('xmas').to_pandas()
first = Search_results.ix[0]

sub_assets = getAssets(tagId=first.id).to_pandas()
print(first)
print(sub_assets.to_string())




