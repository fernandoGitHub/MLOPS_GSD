import tensorflow_data_validation as tfdv
import os
from tensorflow_data_validation.utils import slicing_util
import pandas as pd


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
