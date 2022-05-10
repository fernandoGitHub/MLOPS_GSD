from tensorflow_data_validation.utils import slicing_util

def set_slicing_feature(feature_name):
  slice_fn = slicing_util.get_feature_value_slicer(features={feature_name: None})
  return slice_fn

def set_slicing_options(schema, feature_name):
  slice_fn = set_slicing_feature(feature_name)
  slice_stats_options = tfdv.StatsOptions(schema=schema, slice_functions=slice_fn, infer_type_from_schema=True)
  return slice_stats_options

def calculate_slice_statistics():
  # Convert dataframe to CSV since `slice_functions` works only with `tfdv.generate_statistics_from_csv`
  CSV_PATH = 'slice_sample.csv'
  train_df.to_csv(CSV_PATH)

  # Calculate statistics for the sliced dataset
  slice_stats_options = set_slicing_options(schema, feature_name)
  sliced_stats = tfdv.generate_statistics_from_csv(CSV_PATH, stats_options=slice_stats_options)
  return sliced_stats
