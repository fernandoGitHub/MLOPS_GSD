import os
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


def generate_transformation_function_by_instructions(instructions_dict=None):
  """create_transformation_function_by_instructions(instructions_dict) creates a transform function
  for transforming data as instructed by the instructions_dictionary.
  The function is written to the file transform.py and needs to be imported after this command"""
  
  with open("temp_transform2.py",'w',encoding = 'utf-8') as f:
    f.write(f"def preprocessing_fn (inputs):\n")
    f.write('  \"\"\"Preprocess input columns into transformed columns.\"\"\"\n\n')
    f.write("  # extract the columns and assign to local variables\n")
    
    f.write("  import tensorflow_transform as tft\n")
    f.write("  result = {}\n\n")
    for index, (key, value) in enumerate(instructions_dict.items()):
      new_name = key + "_transformed"
      f.write(f"  {key} = inputs['{key}']\n")

      if value == 'center':
        f.write(f"  {new_name} = {key} - tft.mean({key})\n")
      
      elif value == 'scale_0_1':
        f.write(f"  {new_name} = tft.scale_to_0_1({key})\n")

      elif value == 'apply_vocabulary':
        f.write(f"  {new_name} = tft.compute_and_apply_vocabulary({key})\n")

      f.write(f"  result['{new_name}'] = {new_name}\n\n")
      
    f.write ("\n  # return the transformed data\n")
    f.write ("  return result\n")

    
           
              
    
    
