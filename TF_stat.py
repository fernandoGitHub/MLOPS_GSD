import os
import pandas as pd

import tensorflow_data_validation as tfdv
from tensorflow_data_validation.utils import slicing_util
from tensorflow_metadata.proto.v0.statistics_pb2 import DatasetFeatureStatisticsList


def set_slicing_feature(feature_name):
  """set_slicing_feature(string feature_name) returns a python
  function to be used in set_slicing_options"""
  
  slice_fn = slicing_util.get_feature_value_slicer(features={feature_name: None})
  return slice_fn

def set_slicing_options(slice_fn=None, schema=None, feature_name=None):
  """set_slicing_options(slice_fn, schema, string feature_name) returns an
  options object to be feed inside the statistics slice calculations.
  Schema is a mandatory argument.
  One of the following arguments is mandatory: slice_fn or feature_name."""
  
  if type(feature_name) == type(None) and type(slice_fn) == type(None):
    print('You must provide either slice_fn or feature_name as argument - Aborting function')
    return
  if type(schema) == type(None):
    print('Schema parameter is mandatory - Aborting function')
    return
  if type(feature_name) != type(None) and type(slice_fn) != type(None):
    print('Ambiguous arguments - ignoring feature_name')
  if type(slice_fn) == type(None):
    slice_fn = set_slicing_feature(feature_name)
    
  slice_stats_options = tfdv.StatsOptions(schema=schema, slice_functions=[slice_fn], infer_type_from_schema=True)
  return slice_stats_options


def calculate_slice_statistics(df=None, slice_stats_options=None, schema=None, feature_name=None):
  """calculate_slice_statistics(df, slice_stats_option, schema, string feature_name) returns slice statistics
  based on the slice_stats_options (method_1) or schema and feature name (method_2).
  df (dataframe) is a mandatory argument.
  If slice_stats_option is provided all the other arguments will be ignored"""
  
  if type(df) == type(None):
    print ("A dataframe must be provided. Aborting function!")
    return
  if type(slice_stats_options) == type(None):
    if type(schema) != type(None) and type(feature_name) != type(None):
      slice_stats_options = set_slicing_options(schema=schema, feature_name=feature_name)
    else:
      print ("Not enough arguments for any of the method. Please see help(TF_stat.calculate_slice_statistics) for more info")
      return
  
  # Convert dataframe to CSV since `slice_functions` works only with `tfdv.generate_statistics_from_csv`
  CSV_PATH = 'slice_sample_temp98382918873483294.csv'
  df.to_csv(CSV_PATH)

  # Calculate statistics for the sliced dataset
  sliced_stats = tfdv.generate_statistics_from_csv(CSV_PATH, stats_options=slice_stats_options)
  os.remove(CSV_PATH)
  return sliced_stats


def get_sliced_datasets(sliced_stats=None, df=None, schema=None, feature_name=None):
  if type(sliced_stats) == type(None):
    sliced_stats = calculate_slice_statistics(df=df, schema=schema, feature_name=feature_name)
  
  # For the case the calculation failed
  if type(sliced_stats) == type(None):
    return
  
  return [sliced.name for sliced in sliced_stats.datasets]


def visualize_two_slices_statistics(sliced_stats=None, slice_name_1=None, slice_name_2=None):
  """visualize_two_slices_statistics(sliced_stats, slice_name_1, slice_name_2) displays
  the statistics of the two slices values side by side.
  All the 3 arguments are mandatory"""
  
  if type(sliced_stats) == type(None) or type(slice_name_1) == type(None) or type(slice_name_2) == type(None):
    print ('sliced_stats, slice_name_1, and slice_name_2 are mandatory arguments. Aborting function!')
    return
  
  datasets = get_sliced_datasets(sliced_stats=sliced_stats)
  if not(slice_name_1 in datasets) or not(slice_name_2 in datasets):
    print('The features cannot be found in the dataset. Aborting function!')
    return

  index_1 = datasets.index(slice_name_1)
  feature_stats_list_1 = DatasetFeatureStatisticsList()
  feature_stats_list_1.datasets.extend([sliced_stats.datasets[index_1]])
  
  index_2 = datasets.index(slice_name_2)
  feature_stats_list_2 = DatasetFeatureStatisticsList()
  feature_stats_list_2.datasets.extend([sliced_stats.datasets[index_2]])
  
  # Visualize the two slices side by side
  tfdv.visualize_statistics(lhs_statistics=feature_stats_list_1, rhs_statistics=feature_stats_list_2, lhs_name=slice_name_1, rhs_name=slice_name_2)
  
