import unittest

from cognite.preprocessing import *


class MergeDataframesTestCase(unittest.TestCase):
    def setUp(self):
        l1 = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
        l2 = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
        l3 = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
        self.df1 = pd.DataFrame(l1)
        self.df2 = pd.DataFrame(l2)
        self.df3 = pd.DataFrame(l3)

        self.df1['timestamp'] = self.df1.index * 2000
        self.df2['timestamp'] = self.df2.index * 5000
        self.df3['timestamp'] = self.df3.index * 10000

        self.dataframes = [self.df1, self.df2, self.df3]
        self.merged_df = merge_list_of_dataframes(self.dataframes)

    def test_num_columns(self):
        actual_num_columns = self.df1.shape[1] + self.df2.shape[1] + self.df3.shape[1] - 2
        num_columns = self.merged_df.shape[1]
        self.assertEqual(num_columns, actual_num_columns)

    def test_correct_timestamps(self):
        timestamps = self.merged_df.timestamp.values
        deltas = np.diff(timestamps, 1)
        actual_deltas = np.full((20,), 1000)
        self.assertTrue(np.array_equal(deltas, actual_deltas))


class EvenIndexTestCase(unittest.TestCase):
    def setUp(self):
        l1 = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
        self.df = pd.DataFrame(l1)
        self.df['timestamp'] = [1000, 3000, 8000]
        self.even_index_df = make_index_even(self.df)

    def test_even_index(self):
        timestamps = self.even_index_df.timestamp.values
        even_deltas = np.diff(timestamps, 1) == 1000
        self.assertTrue(np.all(even_deltas))


class FillNanTestCase(unittest.TestCase):
    def setUp(self):
        l1 = [[1, 2, 3, 4], [5, 6, None, 8], [None, 10, 11, 12]]
        l2 = [[None, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
        self.df1 = pd.DataFrame(l1)
        self.df2 = pd.DataFrame(l2)
        self.filled_df = fill_nan(self.df1)
        self.df_leading_nan = fill_nan(self.df2)

    def test_nans_filled(self):
        has_nan = self.filled_df.isna().any().any()
        self.assertFalse(has_nan)

    def test_has_leading_nan(self):
        has_leading_nan = self.df_leading_nan.isna().any()[0]
        self.assertTrue(has_leading_nan)


class RemoveNanColumnsTestCase(unittest.TestCase):
    def setUp(self):
        l1 = [[1, 2, 3, 4], [None, 6, 7, 8], [9, 10, 11, 12]]
        self.df1 = pd.DataFrame(l1)
        self.df1['timestamp'] = self.df1.index * 1000
        self.col_removed_df, self.mask = remove_nan_columns(self.df1)

    def test_correct_mask(self):
        correct_mask = not self.mask[0] and self.mask[1:].all()
        self.assertTrue(correct_mask)

    def test_correct_column_removed(self):
        self.assertFalse(0 in self.col_removed_df.columns)


class RemoveZeroVarColumnsTestCase(unittest.TestCase):
    def setUp(self):
        l1 = [[1, 2, 3, 4], [1, 6, 7, 8], [1, 10, 11, 12]]
        self.df1 = pd.DataFrame(l1)
        self.df1['timestamp'] = self.df1.index * 1000
        self.col_removed_df, self.mask = remove_zero_variance_columns(self.df1)

    def test_correct_mask(self):
        correct_mask = not self.mask[0] and self.mask[1:].all()
        self.assertTrue(correct_mask)

    def test_correct_column_removed(self):
        self.assertFalse(0 in self.col_removed_df.columns)


def suites():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(MergeDataframesTestCase)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(EvenIndexTestCase)
    suite3 = unittest.TestLoader().loadTestsFromTestCase(FillNanTestCase)
    suite4 = unittest.TestLoader().loadTestsFromTestCase(RemoveNanColumnsTestCase)
    suite5 = unittest.TestLoader().loadTestsFromTestCase(RemoveZeroVarColumnsTestCase)
    return [suite1, suite2, suite3, suite4, suite5]
