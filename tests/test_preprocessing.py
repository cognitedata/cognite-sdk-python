import numpy as np
import pandas as pd
import pytest

from cognite import preprocessing


@pytest.fixture(scope="module")
def dfs_no_nan():
    df1 = pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
    df2 = pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
    df3 = pd.DataFrame([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
    df1["timestamp"] = df1.index * 2000
    df2["timestamp"] = df2.index * 5000
    df3["timestamp"] = df3.index * 10000
    return df1, df2, df3


@pytest.fixture(scope="module")
def df_with_zero_var_column():
    return pd.DataFrame([[1, 2, 3], [1, 4, 5], [1, 6, 7]])


@pytest.fixture(scope="module")
def df_with_nan():
    return pd.DataFrame([[1, 2, 3, 4], [5, 6, None, 8], [None, 10, 11, 12]])


@pytest.fixture(scope="module")
def df_with_leading_nan():
    return pd.DataFrame([[None, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])


class TestNormalize:
    def test_zero_mean(self, dfs_no_nan):
        df = dfs_no_nan[0]
        norm_df = preprocessing.normalize(df)
        assert (norm_df.drop("timestamp", axis=1).mean() == 0).all()


class TestMergeDataframes:
    def test_num_columns(self, dfs_no_nan):
        dataframes = list(dfs_no_nan)
        merged_df = preprocessing.merge_list_of_dataframes(dataframes)
        actual_num_columns = dataframes[0].shape[1] + dataframes[1].shape[1] + dataframes[2].shape[1] - 2
        num_columns = merged_df.shape[1]
        assert num_columns == actual_num_columns

    def test_correct_timestamps(self, dfs_no_nan):
        dataframes = list(dfs_no_nan)
        merged_df = preprocessing.merge_list_of_dataframes(dataframes)
        timestamps = merged_df.timestamp.values
        deltas = np.diff(timestamps, 1)
        actual_deltas = np.full((20,), 1000)
        assert np.array_equal(deltas, actual_deltas)

    def test_when_df_is_none(self, dfs_no_nan):
        df1 = dfs_no_nan[0]
        assert preprocessing._merge_dataframes(df1, None).equals(df1)
        assert preprocessing._merge_dataframes(None, df1).equals(df1)


class TestEvenIndex:
    def test_even_index(self, dfs_no_nan):
        df = dfs_no_nan[0]
        df["timestamp"] = [1000, 3000, 8000]
        even_index_df = preprocessing.make_index_even(df)
        timestamps = even_index_df.timestamp.values
        even_deltas = np.diff(timestamps, 1) == 1000
        assert np.all(even_deltas)


class TestFillNan:
    def test_nans_filled(self, df_with_nan):
        filled_df = preprocessing.fill_nan(df_with_nan)
        has_nan = filled_df.isna().any().any()
        assert not has_nan, "NaNs not filled"

    def test_has_leading_nan(self, df_with_leading_nan):
        has_leading_nan = df_with_leading_nan.isna().any()[0]
        assert has_leading_nan, "Leading Nans removed"


class TestRemoveNanColumns:
    @pytest.fixture
    def df_nan_col_removed(self, df_with_nan):
        df = df_with_nan
        df["timestamp"] = df.index * 1000
        col_removed_df, mask = preprocessing.remove_nan_columns(df)
        return col_removed_df, mask

    def test_correct_mask(self, df_nan_col_removed):
        mask = df_nan_col_removed[1]
        correct_mask = not (mask[0] + mask[2]).any() and (mask[1] + mask[3]).all()
        assert correct_mask

    def test_correct_column_removed(self, df_nan_col_removed):
        assert 0 not in df_nan_col_removed[0].columns


class TestRemoveZeroVarColumns:
    @pytest.fixture
    def df_zero_var_removed(self, df_with_zero_var_column):
        df = df_with_zero_var_column
        df["timestamp"] = df.index * 1000
        col_removed_df, mask = preprocessing.remove_zero_variance_columns(df)
        return col_removed_df, mask

    def test_correct_mask(self, df_zero_var_removed):
        mask = df_zero_var_removed[1]
        correct_mask = not mask[0] and mask[1:].all()
        assert correct_mask

    def test_correct_column_removed(self, df_zero_var_removed):
        assert 0 not in df_zero_var_removed[0].columns


class TestPreprocess:
    @pytest.fixture(scope="class")
    def df_nans_uneven(self):
        df = pd.DataFrame([[None, 2, 3, 4], [5, None, 7, 8], [9, 10, 11, 12]])
        df["timestamp"] = [1000, 3000, 8000]
        return df

    def test_preprocess(self, df_nans_uneven):
        with pytest.warns(UserWarning):
            pp_df, mask = preprocessing.preprocess(df_nans_uneven)
        timestamps = pp_df.timestamp.values
        even_deltas = np.diff(timestamps, 1) == 1000
        assert not pp_df.isnull().values.any()
        assert np.all(even_deltas)
        assert mask[0] == False
        assert mask[1:].all()

    def test_preprocess_remove_leading_nan(self, df_nans_uneven):
        pp_df, mask = preprocessing.preprocess(df_nans_uneven, remove_leading_nan_rows=True)
        assert mask.all()
        assert not pp_df.isnull().values.any()

    def test_preprocess_center_and_scale(self, df_nans_uneven):
        pp_df, mask = preprocessing.preprocess(df_nans_uneven, center_and_scale=True, remove_leading_nan_rows=True)
        assert (pp_df.drop("timestamp", axis=1).mean().round() == 0).all()
