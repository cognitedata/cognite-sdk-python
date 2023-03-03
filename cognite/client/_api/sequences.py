
import copy
import math
from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Sequence, SequenceAggregate, SequenceData, SequenceDataList, SequenceFilter, SequenceList, SequenceUpdate
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._identifier import Identifier, IdentifierSequence
if TYPE_CHECKING:
    import pandas

class SequencesAPI(APIClient):
    _RESOURCE_PATH = '/sequences'

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.data = SequencesDataAPI(self, *args, **kwargs)

    def __call__(self, chunk_size=None, name=None, external_id_prefix=None, metadata=None, asset_ids=None, asset_subtree_ids=None, asset_subtree_external_ids=None, data_set_ids=None, data_set_external_ids=None, created_time=None, last_updated_time=None, limit=None):
        asset_subtree_ids_processed = None
        if (asset_subtree_ids or asset_subtree_external_ids):
            asset_subtree_ids_processed = IdentifierSequence.load(asset_subtree_ids, asset_subtree_external_ids).as_dicts()
        data_set_ids_processed = None
        if (data_set_ids or data_set_external_ids):
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = SequenceFilter(name=name, metadata=metadata, external_id_prefix=external_id_prefix, asset_ids=asset_ids, asset_subtree_ids=asset_subtree_ids_processed, created_time=created_time, last_updated_time=last_updated_time, data_set_ids=data_set_ids_processed).dump(camel_case=True)
        return self._list_generator(list_cls=SequenceList, resource_cls=Sequence, method='POST', chunk_size=chunk_size, filter=filter, limit=limit)

    def __iter__(self):
        return cast(Iterator[Sequence], self())

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=SequenceList, resource_cls=Sequence, identifiers=identifiers)

    def retrieve_multiple(self, ids=None, external_ids=None, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(list_cls=SequenceList, resource_cls=Sequence, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids)

    def list(self, name=None, external_id_prefix=None, metadata=None, asset_ids=None, asset_subtree_ids=None, asset_subtree_external_ids=None, data_set_ids=None, data_set_external_ids=None, created_time=None, last_updated_time=None, limit=25):
        asset_subtree_ids_processed = None
        if (asset_subtree_ids or asset_subtree_external_ids):
            asset_subtree_ids_processed = IdentifierSequence.load(asset_subtree_ids, asset_subtree_external_ids).as_dicts()
        data_set_ids_processed = None
        if (data_set_ids or data_set_external_ids):
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = SequenceFilter(name=name, metadata=metadata, external_id_prefix=external_id_prefix, asset_ids=asset_ids, asset_subtree_ids=asset_subtree_ids_processed, created_time=created_time, last_updated_time=last_updated_time, data_set_ids=data_set_ids_processed).dump(camel_case=True)
        return self._list(list_cls=SequenceList, resource_cls=Sequence, method='POST', filter=filter, limit=limit)

    def aggregate(self, filter=None):
        return self._aggregate(filter=filter, cls=SequenceAggregate)

    @overload
    def create(self, sequence):
        ...

    @overload
    def create(self, sequence):
        ...

    def create(self, sequence):
        utils._auxiliary.assert_type(sequence, 'sequences', [SequenceType, Sequence])
        if isinstance(sequence, SequenceType):
            sequence = [self._clean_columns(seq) for seq in sequence]
        else:
            sequence = self._clean_columns(sequence)
        return self._create_multiple(list_cls=SequenceList, resource_cls=Sequence, items=sequence)

    def _clean_columns(self, sequence):
        sequence = copy.copy(sequence)
        sequence.columns = [{k: v for (k, v) in utils._auxiliary.convert_all_keys_to_camel_case(col).items() if (k in ['externalId', 'valueType', 'metadata', 'name', 'description'])} for col in cast(List, sequence.columns)]
        for i in range(len(sequence.columns)):
            if (not sequence.columns[i].get('externalId')):
                sequence.columns[i]['externalId'] = ('column' + str(i))
            if sequence.columns[i].get('valueType'):
                sequence.columns[i]['valueType'] = sequence.columns[i]['valueType'].upper()
        return sequence

    def delete(self, id=None, external_id=None):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id, external_ids=external_id), wrap_ids=True)

    @overload
    def update(self, item):
        ...

    @overload
    def update(self, item):
        ...

    def update(self, item):
        return self._update_multiple(list_cls=SequenceList, resource_cls=Sequence, update_cls=SequenceUpdate, items=item)

    def search(self, name=None, description=None, query=None, filter=None, limit=100):
        return self._search(list_cls=SequenceList, search={'name': name, 'description': description, 'query': query}, filter=(filter or {}), limit=limit)

class SequencesDataAPI(APIClient):
    _DATA_PATH = '/sequences/data'

    def __init__(self, sequences_api, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._sequences_api = sequences_api
        self._SEQ_POST_LIMIT_ROWS = 10000
        self._SEQ_POST_LIMIT_VALUES = 100000
        self._SEQ_RETRIEVE_LIMIT = 10000

    def insert(self, rows, column_external_ids, id=None, external_id=None):
        if isinstance(rows, SequenceData):
            column_external_ids = rows.column_external_ids
            rows = [{'rowNumber': k, 'values': v} for (k, v) in rows.items()]
        if isinstance(rows, dict):
            all_rows: Union[(Dict, SequenceType)] = [{'rowNumber': k, 'values': v} for (k, v) in rows.items()]
        elif (isinstance(rows, SequenceType) and (len(rows) > 0) and isinstance(rows[0], dict)):
            all_rows = rows
        elif (isinstance(rows, SequenceType) and ((len(rows) == 0) or isinstance(rows[0], tuple))):
            all_rows = [{'rowNumber': k, 'values': v} for (k, v) in rows]
        else:
            raise ValueError("Invalid format for 'rows', expected a list of tuples, list of dict or dict")
        base_obj = Identifier.of_either(id, external_id).as_dict()
        base_obj.update(self._process_columns(column_external_ids))
        if (len(all_rows) > 0):
            rows_per_request = min(self._SEQ_POST_LIMIT_ROWS, int((self._SEQ_POST_LIMIT_VALUES / len(all_rows[0]['values']))))
        else:
            rows_per_request = self._SEQ_POST_LIMIT_ROWS
        row_objs = [{'rows': all_rows[i:(i + rows_per_request)]} for i in range(0, len(all_rows), rows_per_request)]
        tasks = [({**base_obj, **rows},) for rows in row_objs]
        summary = utils._concurrency.execute_tasks(self._insert_data, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks()

    def insert_dataframe(self, dataframe, id=None, external_id=None):
        dataframe = dataframe.replace({math.nan: None})
        data = [(v[0], list(v[1:])) for v in dataframe.itertuples()]
        column_external_ids = [str(s) for s in dataframe.columns]
        self.insert(rows=data, column_external_ids=column_external_ids, id=id, external_id=external_id)

    def _insert_data(self, task):
        self._post(url_path=self._DATA_PATH, json={'items': [task]})

    def delete(self, rows, id=None, external_id=None):
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj['rows'] = rows
        self._post(url_path=(self._DATA_PATH + '/delete'), json={'items': [post_obj]})

    def delete_range(self, start, end, id=None, external_id=None):
        sequence = self._sequences_api.retrieve(id=id, external_id=external_id)
        assert (sequence is not None)
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj.update(self._process_columns(column_external_ids=[sequence.column_external_ids[0]]))
        post_obj.update({'start': start, 'end': end})
        for (data, _) in self._fetch_data(post_obj):
            if data:
                self.delete(rows=[r['rowNumber'] for r in data], external_id=external_id, id=id)

    def retrieve(self, start, end, column_external_ids=None, external_id=None, id=None, limit=None):
        post_objs = IdentifierSequence.load(id, external_id).as_dicts()

        def _fetch_sequence(post_obj: Dict[(str, Any)]) -> SequenceData:
            post_obj.update(self._process_columns(column_external_ids=column_external_ids))
            post_obj.update({'start': start, 'end': end, 'limit': limit})
            seqdata: List = []
            columns: List = []
            for (data, columns) in self._fetch_data(post_obj):
                seqdata.extend(data)
            return SequenceData(id=post_obj.get('id'), external_id=post_obj.get('externalId'), rows=seqdata, columns=columns)
        tasks_summary = utils._concurrency.execute_tasks(_fetch_sequence, [(x,) for x in post_objs], max_workers=self._config.max_workers)
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]
        results = tasks_summary.joined_results()
        if (len(post_objs) == 1):
            return results[0]
        else:
            return SequenceDataList(results)

    def retrieve_latest(self, id=None, external_id=None, column_external_ids=None, before=None):
        identifier = Identifier.of_either(id, external_id).as_dict()
        res = self._do_request('POST', (self._DATA_PATH + '/latest'), json={**identifier, 'before': before, 'columns': column_external_ids}).json()
        return SequenceData(id=res['id'], external_id=res.get('external_id'), rows=res['rows'], columns=res['columns'])

    def retrieve_dataframe(self, start, end, column_external_ids=None, external_id=None, column_names=None, id=None, limit=None):
        if (isinstance(external_id, List) or isinstance(id, List) or ((id is not None) and (external_id is not None))):
            column_names_default = 'externalId|columnExternalId'
        else:
            column_names_default = 'columnExternalId'
        return self.retrieve(start, end, column_external_ids, external_id, id, limit).to_pandas(column_names=(column_names or column_names_default))

    def _fetch_data(self, task):
        remaining_limit = task.get('limit')
        columns: List[str] = []
        cursor = None
        if (task['end'] == (- 1)):
            task['end'] = None
        while True:
            task['limit'] = min(self._SEQ_RETRIEVE_LIMIT, (remaining_limit or self._SEQ_RETRIEVE_LIMIT))
            task['cursor'] = cursor
            resp = self._post(url_path=(self._DATA_PATH + '/list'), json=task).json()
            data = resp['rows']
            columns = (columns or resp['columns'])
            (yield (data, columns))
            cursor = resp.get('nextCursor')
            if remaining_limit:
                remaining_limit -= len(data)
            if ((not cursor) or ((remaining_limit is not None) and (remaining_limit <= 0))):
                break

    def _process_columns(self, column_external_ids):
        if (column_external_ids is None):
            return {}
        return {'columns': column_external_ids}
