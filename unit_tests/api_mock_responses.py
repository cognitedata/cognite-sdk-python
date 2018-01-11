# CONSTANTS
# TEST_API_KEY = 'm7SSQZ8ug72b1cUWCLMfc3uy9lkHBeyO'
# TEST_PROJECT = 'akerbp'

import json
from cognite.data_objects import *


tag_matching_response = {
    "data": {
        "items": [
            {
                "matches": [
                    {
                        "platform": "SKA",
                        "score": 0,
                        "tagId": "SKAP_18PI2317/Y/10sSAMP"
                    },
                    {
                        "platform": "SKA",
                        "score": 0,
                        "tagId": "SKAP_18PI2317/Y/10sSAMP_DAY_AVG"
                    },
                    {
                        "platform": "SKA",
                        "score": 0,
                        "tagId": "SKAP_18PI2317/Y/10sSAMP_HOUR_AVG"
                    }
                ],
                "tagId": "18pi2317"
            }
        ]
    }
}
tag_matching_response = TagMatchingObject(tag_matching_response)



assets_response = {
    'data': {
        'items': [
            {'id': 2015768764468795,
             'name': '13WA0701',
             'parentId': 1516016186968133,
             'description': 'XMAS TREE (SLOT 1 - DWI02)',
             'metadata': {
                 'Area': 'M210',
                 'Contractor': 'FM',
                 'CreatedBy': '7cbec2e8-2b4d-4682-8be9-2cea4a835cb3',
                 'CreatedDate': '2017-07-05 11:45:56.677000',
                 'DateInstalled': '2017-01-26',
                 'Description': 'XMAS TREE (SLOT 1 - DWI02)',
                 'ExCode': 'NA',
                 'FieldEquipmentFlag': 'Y',
                 'FireArea': 'NA',
                 'GasGroup': 'NA',
                 'IPGrade': 'NA',
                 'Installation': 'IAA',
                 'LoadNo': '42FA5221-8BD6-4227-994C-7B5FC236DBE0',
                 'Manufacturer': 'FM',
                 'ModProject': 'OPERATIONS',
                 'ModelNo': 'NA',
                 'MountedOn': '',
                 'ParentTag': '13WD0701',
                 'PartNo': 'P2000062748 & P2000062761',
                 'ProjectCode': 'DN02',
                 'PurchaseOrder': '',
                 'Remark': 'NA',
                 'SequenceNo': '701',
                 'SerialNo': 'TBC',
                 'System': '13',
                 'TagCategory': 'MAIN_EQUIPMENT',
                 'TagDiscipline': 'U',
                 'TagModCode': 'MOD',
                 'TagNo': '13WA0701',
                 'TagStatus': 'ASBUILT',
                 'TagType': 'WA',
                 'TemperatureClass': '',
                 'ThermodynamicMonitoring': 'N',
                 'VibrationMonitoring': 'N',
                 'WeightFlag': 'Y',
                 'WeightNet': '5850.0',
                 'WeightOperational': '5850.0'}
             }
        ]
    }
}
assets_response = AssetSearchObject(assets_response)


timeseries_response = {
    'data': {
        'items': [
            {'tagId': 'SKAP_18PI2117/Y/10sSAMP',
             'datapoints': [
                 {'timestamp': 1514452010000, 'value': 98.52658081054688},
                 {'timestamp': 1514452020000, 'value': 98.55802154541016},
                 {'timestamp': 1514452030000, 'value': 98.53913879394531},
                 {'timestamp': 1514452040000, 'value': 98.56653594970703},
                 {'timestamp': 1514452050000, 'value': 98.53701782226562},
                 {'timestamp': 1514452060000, 'value': 98.5904541015625},
                 {'timestamp': 1514452070000, 'value': 98.5936279296875},
                 {'timestamp': 1514452080000, 'value': 98.57329559326172},
                 {'timestamp': 1514452090000, 'value': 98.60504913330078},
                 {'timestamp': 1514452100000, 'value': 98.5923843383789},
             ]
             }
        ]
    }
}

timeseries_response = [DatapointsObject(timeseries_response)]

similarity_search_response = {
    'data': {
        'items': [
            {'from': 1360969200000, 'to': 1360969500000},
            {'from': 1360987200000, 'to': 1360987500000}
        ]
    }
}

similarity_search_response = SimilaritySearchObject(similarity_search_response)
