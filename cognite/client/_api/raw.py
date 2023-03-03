from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Database, DatabaseList, Row, RowList, Table, TableList
from cognite.client.utils._auxiliary import is_unlimited, local_import
from cognite.client.utils._identifier import Identifier


class RawAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.databases = RawDatabasesAPI(*args, **kwargs)
        self.tables = RawTablesAPI(*args, **kwargs)
        self.rows = RawRowsAPI(*args, **kwargs)


class RawDatabasesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs"

    def __call__(self, chunk_size=None, limit=None):
        return self._list_generator(
            list_cls=DatabaseList, resource_cls=Database, chunk_size=chunk_size, method="GET", limit=limit
        )

    def __iter__(self):
        return cast(Iterator[Database], self())

    @overload
    def create(self, name):
        ...

    @overload
    def create(self, name):
        ...

    def create(self, name):
        utils._auxiliary.assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            items: Union[(Dict[(str, Any)], List[Dict[(str, Any)]])] = {"name": name}
        else:
            items = [{"name": n} for n in name]
        return self._create_multiple(list_cls=DatabaseList, resource_cls=Database, items=items)

    def delete(self, name, recursive=False):
        utils._auxiliary.assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            name = [name]
        items = [{"name": n} for n in name]
        chunks = utils._auxiliary.split_into_chunks(items, self._DELETE_LIMIT)
        tasks = [
            {"url_path": (self._RESOURCE_PATH + "/delete"), "json": {"items": chunk, "recursive": recursive}}
            for chunk in chunks
        ]
        summary = utils._concurrency.execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=(lambda task: task["json"]["items"]), task_list_element_unwrap_fn=(lambda el: el["name"])
        )

    def list(self, limit=25):
        return self._list(list_cls=DatabaseList, resource_cls=Database, method="GET", limit=limit)


class RawTablesAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables"

    def __call__(self, db_name, chunk_size=None, limit=None):
        for tb in self._list_generator(
            list_cls=TableList,
            resource_cls=Table,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            chunk_size=chunk_size,
            method="GET",
            limit=limit,
        ):
            (yield self._set_db_name_on_tables(tb, db_name))

    @overload
    def create(self, db_name, name):
        ...

    @overload
    def create(self, db_name, name):
        ...

    def create(self, db_name, name):
        utils._auxiliary.assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            items: Union[(Dict[(str, Any)], List[Dict[(str, Any)]])] = {"name": name}
        else:
            items = [{"name": n} for n in name]
        tb = self._create_multiple(
            list_cls=TableList,
            resource_cls=Table,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            items=items,
        )
        return self._set_db_name_on_tables(tb, db_name)

    def delete(self, db_name, name):
        utils._auxiliary.assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            name = [name]
        items = [{"name": n} for n in name]
        chunks = utils._auxiliary.split_into_chunks(items, self._DELETE_LIMIT)
        tasks = [
            {
                "url_path": (utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name) + "/delete"),
                "json": {"items": chunk},
            }
            for chunk in chunks
        ]
        summary = utils._concurrency.execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=(lambda task: task["json"]["items"]), task_list_element_unwrap_fn=(lambda el: el["name"])
        )

    def list(self, db_name, limit=25):
        tb = self._list(
            list_cls=TableList,
            resource_cls=Table,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name),
            method="GET",
            limit=limit,
        )
        return cast(TableList, self._set_db_name_on_tables(tb, db_name))

    def _set_db_name_on_tables(self, tb, db_name):
        if isinstance(tb, Table):
            tb._db_name = db_name
            return tb
        elif isinstance(tb, TableList):
            for t in tb:
                t._db_name = db_name
            return tb
        raise TypeError("tb must be Table or TableList")


class RawRowsAPI(APIClient):
    _RESOURCE_PATH = "/raw/dbs/{}/tables/{}/rows"

    def __init__(self, config, api_version=None, cognite_client=None):
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 5000
        self._LIST_LIMIT = 10000

    def __call__(
        self,
        db_name,
        table_name,
        chunk_size=None,
        limit=None,
        min_last_updated_time=None,
        max_last_updated_time=None,
        columns=None,
    ):
        return self._list_generator(
            list_cls=RowList,
            resource_cls=Row,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            chunk_size=chunk_size,
            method="GET",
            limit=limit,
            filter={
                "minLastUpdatedTime": min_last_updated_time,
                "maxLastUpdatedTime": max_last_updated_time,
                "columns": self._make_columns_param(columns),
            },
        )

    def insert(self, db_name, table_name, row, ensure_parent=False):
        chunks = self._process_row_input(row)
        tasks = [
            {
                "url_path": utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
                "json": {"items": chunk},
                "params": {"ensureParent": ensure_parent},
            }
            for chunk in chunks
        ]
        summary = utils._concurrency.execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=(lambda task: task["json"]["items"]),
            task_list_element_unwrap_fn=(lambda row: row.get("key")),
        )

    def insert_dataframe(self, db_name, table_name, dataframe, ensure_parent=False):
        df_dict = dataframe.to_dict(orient="index")
        rows = [Row(key=key, columns=cols) for (key, cols) in df_dict.items()]
        self.insert(db_name=db_name, table_name=table_name, row=rows, ensure_parent=ensure_parent)

    def _process_row_input(self, row):
        utils._auxiliary.assert_type(row, "row", [Sequence, dict, Row])
        rows = []
        if isinstance(row, dict):
            for (key, columns) in row.items():
                rows.append({"key": key, "columns": columns})
        elif isinstance(row, list):
            for elem in row:
                if isinstance(elem, Row):
                    rows.append(elem.dump(camel_case=True))
                else:
                    raise TypeError("list elements must be Row objects.")
        elif isinstance(row, Row):
            rows.append(row.dump(camel_case=True))
        return utils._auxiliary.split_into_chunks(rows, self._CREATE_LIMIT)

    def delete(self, db_name, table_name, key):
        utils._auxiliary.assert_type(key, "key", [str, Sequence])
        if isinstance(key, str):
            key = [key]
        items = [{"key": k} for k in key]
        chunks = utils._auxiliary.split_into_chunks(items, self._DELETE_LIMIT)
        tasks = [
            {
                "url_path": (
                    utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name) + "/delete"
                ),
                "json": {"items": chunk},
            }
            for chunk in chunks
        ]
        summary = utils._concurrency.execute_tasks(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=(lambda task: task["json"]["items"]), task_list_element_unwrap_fn=(lambda el: el["key"])
        )

    def retrieve(self, db_name, table_name, key):
        return self._retrieve(
            cls=Row,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
            identifier=Identifier(key),
        )

    def list(self, db_name, table_name, min_last_updated_time=None, max_last_updated_time=None, columns=None, limit=25):
        if is_unlimited(limit):
            cursors = self._get(
                url_path=utils._auxiliary.interpolate_and_url_encode(
                    "/raw/dbs/{}/tables/{}/cursors", db_name, table_name
                ),
                params={
                    "minLastUpdatedTime": min_last_updated_time,
                    "maxLastUpdatedTime": max_last_updated_time,
                    "numberOfCursors": self._config.max_workers,
                },
            ).json()["items"]
        else:
            cursors = [None]
        tasks = [
            dict(
                list_cls=RowList,
                resource_cls=Row,
                resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, db_name, table_name),
                method="GET",
                filter={
                    "columns": self._make_columns_param(columns),
                    "minLastUpdatedTime": min_last_updated_time,
                    "maxLastUpdatedTime": max_last_updated_time,
                },
                limit=limit,
                initial_cursor=cursor,
            )
            for cursor in cursors
        ]
        summary = utils._concurrency.execute_tasks(self._list, tasks, max_workers=self._config.max_workers)
        if summary.exceptions:
            raise summary.exceptions[0]
        return RowList(summary.joined_results())

    def _make_columns_param(self, columns):
        if columns is None:
            return None
        if not isinstance(columns, List):
            raise ValueError("Expected a list for argument columns")
        if len(columns) == 0:
            return ","
        else:
            return ",".join([str(x) for x in columns])

    def retrieve_dataframe(
        self, db_name, table_name, min_last_updated_time=None, max_last_updated_time=None, columns=None, limit=25
    ):
        pd = cast(Any, local_import("pandas"))
        rows = self.list(db_name, table_name, min_last_updated_time, max_last_updated_time, columns, limit)
        idx = [r.key for r in rows]
        cols = [r.columns for r in rows]
        return pd.DataFrame(cols, index=idx)
