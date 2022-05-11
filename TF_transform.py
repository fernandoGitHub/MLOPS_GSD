import tensorflow as tf
import tensorflow_transform as tft
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import schema_utils

def create_schema_dict_by_instructions(instructions_dict=None):
  """ create_schema_dict_by_instructions(instructions_dict) creates a tensorflow schema dictionary
  for simple cases. The input dictionary should contain a tuple
  feature name as a string : feature type as a string
  The schema will include only Fixed Length Features
  The valid feature types are int, float and string
  The function returns a TF schema dict"""
  
  if type (instructions_dict) == type(None):
    print ('Dictionary is missing. Aborting function!')
  
  types_dict = {'int':tf.int32, 'float':tf.float32, 'string':tf.string}
  schema_dict = {}
  for index, (key, value) in enumerate(instructions_dict.items()):
    schema_dict[key] = tf.io.FixedLenFeature([], types_dict[value])
  
  return schema_dict
                                             
def create_schema_by_instructions(instructions_dict=None):
  """ create_schema_by_instructions(instructions_dict) creates a schema
  for simple cases. The input dictionary should contain a tuple
  feature name as a string : feature type as a string
  The schema will include only Fixed Length Features
  The valid feature types are int, float and string
  The function returns both a metadata and its schema"""
  
  if type (instructions_dict) == type(None):
    print ('Dictionary is missing. Aborting function!')
  
  schema_dict = create_schema_dict_by_instructions(instructions_dict)
  data_metadata = dataset_metadata.DatasetMetadata(schema_utils.schema_from_feature_spec(schema_dict))
  
  return data_metadata, data_metadata._schema

    
