from cognite.client.data_classes.data_modeling.graphql import DMLApplyResult


def test_dmlapplyresult_is_str_convertable() -> None:
    # Bug prior to 7.20.1 would try to convert non-integer timestamps 'created_time' and 'last_updated_time'
    # from millis since epoch and fail (already string-formatted nicely):
    res = DMLApplyResult(
        space="test-space",
        external_id="test-xid",
        version="v1",
        name="foo",
        description="bar",
        created_time="2024-02-18T22:49:54.932Z",
        last_updated_time="2024-02-18T22:55:20.660Z",
    )
    assert str(res)
