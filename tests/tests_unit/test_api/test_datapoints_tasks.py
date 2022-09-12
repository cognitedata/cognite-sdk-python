class Test_SingleTSQueryValidator:

    @pytest.mark.parametrize("limit, exp_limit", [(-1, None), (0, 0), (1, 1), (math.inf, None), (None, None)])
    def test_query_valid_limits(limit, exp_limit):
        ts_query = _SingleTSQueryValidator(DatapointsQuery(id=0, limit=limit)).validate_and_create_single_queries()
        assert exp_limit == ts_query.limit
