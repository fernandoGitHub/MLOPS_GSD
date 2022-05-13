import tensorflow as tf
import tensorflow_data_validation as tfdv
from tfx import v1 as tfx
from tfx.types import standard_artifacts
from tensorflow_metadata.proto.v0 import schema_pb2

import os
import pprint
pp = pprint.PrettyPrinter()

def display_catalog(short=False):
  if short:
    print("get_schema_uri(schema_gen=None")
    print("load_schema(schema_gen=None)")
    print("get_features_names_in_schema(schema_gen=None, schema=None)")
    print("load_schema(schema_gen=None)")
    print("display_schema(schema=None)")
    print("set_int_domain(min, max, schema=None, feature_name=None)")
    print("append_environments(environments=None)")
    print("remove_feature_from_environment(schema, feature_name, environment)")
    print("write_schema_to_file(schema, url_dir, filename)")
  else:
    help(get_schema_uri)
    help(load_schema)
    help(get_features_names_in_schema)
    help(load_schema)
    help(display_schema)
    help(set_int_domain)
    help(append_environments)
    help(remove_feature_from_environment)
    help(write_schema_to_file)

    
def get_schema_uri(schema_gen=None):
  """get_schema_uri(schema_gen) returns the location of the schema info"""

  if type(schema_gen) == type(None):
    print("schema_gen parameter is mandatory. Aborting function!")
    return

  return schema_gen.outputs['schema']._artifacts[0].uri


def load_schema(schema_gen=None):
  """load_schema(shema_gen) reads a schema stored as a schema.pbtxt file
  and returns it as a schema object"""

  schema_uri = get_schema_uri(schema_gen=schema_gen)
  return tfdv.load_schema_text(os.path.join(schema_uri, 'schema.pbtxt'))


def get_features_names_in_schema(schema_gen=None, schema=None):
  """get_features_names_in_schema(schema_gen, schema) returns a list of 
  features names. Only one parameter is necessary. In case both parameters
  are provided, only the schema parameter will be used"""

  if type(schema_gen) == type(None) and type(schema) == type(None):
    print('At least one parameter needs to be specified. Aborting function!')
    return

  if type(schema) == type(None):
    schema = load_schema(schema_gen=schema_gen)

  return [feature.name for i, feature in enumerate(schema.feature)]


def display_schema(schema=None):
  """display_schema(schema) displays the schema"""
  tfdv.display_schema(schema)
  

def set_int_domain(min, max, schema=None, feature_name=None):
  """set_int_domain(min, max, schema, feature_name) sets the 
  domain limits for an Int Type.
  The function returns the new schema object"""
  
  if not (feature_name in get_features_names_in_schema(schema=schema)):
    print("This feature is not part of the given schema. Aborting function!")
    return schema

  tfdv.set_domain(schema, 'age', schema_pb2.IntDomain(name='age', min=17, max=90))

  return schema


def append_environments(schema, environments):
  """append_environment(schema, environment) gets a list of environments
  and appends them to the schema returning the updated schema"""

  for env in environments:
    schema.default_environment.append(env) 

  return schema


def remove_feature_from_environment(schema, feature_name, environment):
  """remove_feature_from_environment(schema, feature_name, environment) removes
  the given feature from the given environment.
  The function returns the new schema object"""

  tfdv.get_feature(schema, feature_name).not_in_environment.append(environment)

  return schema


def write_schema_to_file(schema, url_dir, filename):
  """write_schema_to_file(shema, url_dir, filename) saves the schema
  to a file. If necessary, the directories needed for the url are
  created."""

  if not os.path.isdir(url_dir):
    os.makedirs(url_dir)

  url_file = os.path.join(url_dir, filename)
  tfdv.write_schema_text(schema, url_file)
