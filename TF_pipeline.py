import tensorflow as tf
from tfx import v1 as tfx
from tfx.orchestration.experimental.interactive.interactive_context import InteractiveContext
from google.protobuf.json_format import MessageToDict
import os
import pprint
pp = pprint.PrettyPrinter()

def display_catalog(short=False):
  if short:
    print("generate_examples(context=None, input_base=None)")
    print("get_examples_artifact_data(example_gen=None, display=False)")
    print("get_records_from_pipeline_dataset(uri, num_records, display=True)")
    print("generate_statistics(context=None, examples_gen=None, input_base=None, display=False)")
    print("generate_schema(context=None, statistics_gen=None, input_base=None, display=False)")
    print("generate_validation(context=None, statistics_gen=None, schema_gen=None, input_base=None, display=False)")
    print("show_schema(context, schema_gen)")
    print("show_validation(context, example_validator)")
    print("import_schema_from_file(context, schema_file, display=False)")
  else:
    help(generate_examples)
    help(get_examples_artifact_data)
    help(get_records_from_pipeline_dataset)
    help(generate_statistics)
    help(generate_schema)
    help(generate_validation)
    help(show_schema)
    help(show_validation)
    help(import_schema_from_file)
    
    
def generate_examples(context=None, input_base=None):
  """generate examples(context, input_base) sends all the data in the
  input_base directory to the TFX Example Generator Pipeline.
  The function returns the resulting example_gen"""

  if type(input_base) == type(None) or type(context) == type(None):
    print('Both parameters are mandatory. Aborting execution!')
    return

  # This command is necessary as all the files in the folder need to have the same header/columns
  # Not clear where the .ipynb_checkpoints folder came from
  if os.path.isdir('/content/data/census_data/.ipnby_checkpoints'):
    os.rmdir('/content/data/census_data/.ipynb_checkpoints')

  example_gen = tfx.components.CsvExampleGen(input_base=input_base)

  # Execute the component
  context.run(example_gen)

  return example_gen


def get_examples_artifact_data(example_gen=None, display=False):
  """get_examples_artifact_data(example_gen, display) returns a list to the
  generated data and the artifacts data output uri"""

  if type(example_gen) == type(None):
    print ('The example_gen argument is mandatory. Aborting function!')
    return

  artifact = example_gen.outputs['examples'].get()[0]
  
  if display:
    # print split names and uri
    print(f'split names: {artifact.split_names}')
    print(f'artifact uri: {artifact.uri}')
  
  return artifact.split_names, artifact.uri


def get_records_from_pipeline_dataset(uri, num_records, display=True):
  '''Extracts records from the given dataset.
  Args:
  uri of datasets saved by ExampleGen
  num_records (int): number of records to preview
  '''
  # Get the list of files in this directory (all compressed TFRecord files)
  tfrecord_filenames = [os.path.join(uri, name)
                      for name in os.listdir(uri)]

  # Create a `TFRecordDataset` to read these files
  dataset = tf.data.TFRecordDataset(tfrecord_filenames, compression_type="GZIP")
  
  # initialize an empty list
  records = []
    
  # Use the `take()` method to specify how many records to get
  for tfrecord in dataset.take(num_records):
        
    # Get the numpy property of the tensor
    serialized_example = tfrecord.numpy()
        
    # Initialize a `tf.train.Example()` to read the serialized data
    example = tf.train.Example()
        
    # Read the example data (output is a protocol buffer message)
    example.ParseFromString(serialized_example)
        
    # convert the protocol bufffer message to a Python dictionary
    example_dict = (MessageToDict(example))
        
    # append to the records list
    records.append(example_dict)
    
  if display:
    # Print the output
    pp.pprint(records)
        
  return records


def generate_statistics(context=None, examples_gen=None, input_base=None, display=False):
  """generate_statistics(context, examples_gen, input_base, display) generates statistics
  from generated examples. The function requires context and either examples_gen
  or input_base. If display = True the statistics will be displayed.
  The function returns statistics_gen"""

  if type(context) == type(None):
    print ("Context parameter is mandatory. Aborting execution!!!")
    return

  if type(examples_gen) == type(None) and type(input_base) == type(None):
    print ('Either examples_gen or input_base needs to be defined. Aborting execution!')
    return

  if type(examples_gen) == type(None):
    examples_gen = generate_examples(context=context, input_base=input_base)

  # Instantiate StatisticsGen with the ExampleGen ingested dataset
  statistics_gen = tfx.components.StatisticsGen(examples=examples_gen.outputs['examples'])

  # Execute the component
  context.run(statistics_gen)

  # Show the output statistics
  if display:
    context.show(statistics_gen.outputs['statistics'])

  return statistics_gen


def generate_schema(context=None, statistics_gen=None, input_base=None, display=False):
  """generate_schema(context, statistics_gen, input_base, display) generates a scheme
  from generated examples. The function requires context and either statistics_gen
  or input_base. If display = True the statistics and scheme will be displayed.
  The function returns schema_gen"""

  if type(context) == type(None):
    print ("Context parameter is mandatory. Aborting execution!!!")
    return

  if type(statistics_gen) == type(None) and type(input_base) == type(None):
    print ('Either statistics_gen or input_base needs to be defined. Aborting execution!')
    return

  if type(statistics_gen) == type(None):
    statistics_gen = generate_statistics(context=context, input_base=input_base, display=display)

  # Instantiate SchemaGen with the StatisticsGen ingested dataset
  schema_gen = tfx.components.SchemaGen(statistics=statistics_gen.outputs['statistics'],)

  # Execute the component
  context.run(schema_gen)

  # Show the output statistics
  if display:
    show_schema(context=context, schema_gen=schema_gen)

  return schema_gen


def show_schema(context, schema_gen):
  """display_schema displays the schema from a schema_gen object."""

  schema = schema_gen.outputs['schema']

  context.show(schema)

  
def generate_validation(context=None, statistics_gen=None, schema_gen=None, input_base=None, display=False):
  """generate_schema(context, statistics_gen, input_base, display) generates a scheme
  from generated examples. The function requires context and either statistics_gen
  or input_base. If display = True results will be displayed.
  The function returns example_validator"""

  if type(context) == type(None):
    print ("Context parameter is mandatory. Aborting execution!!!")
    return

  if type(statistics_gen) == type(None) and type(schema_gen) == type (None) and type(input_base) == type(None):
    print ('Either statistics_gen/scheme_gen or input_base need to be defined. Aborting execution!')
    return

  if type(statistics_gen) == type(None):
    statistics_gen = generate_statistics(context=context, input_base=input_base, display=display)

  if type(schema_gen) == type(None):
    schema_gen = generate_schema(context=context, statistics_gen=statistics_gen, display=display)

  # Instantiate ExampleValidator with the StatisticsGen and SchemaGen ingested data
  example_validator = tfx.components.ExampleValidator(statistics=statistics_gen.outputs['statistics'], schema=schema_gen.outputs['schema'])

  # Execute the component
  context.run(example_validator)

  # Show the output statistics
  if display:
    show_validation(context=context, example_validator=example_validator)

  return example_validator

def show_validation(context, example_validator):
  """show_validation(context, validation) displays the validation results"""

  context.show(example_validator.outputs['anomalies'])

  
def import_schema_from_file(context, schema_file, display=False):
  """import_schema_from_file (context, schema_file) inputs the schema from a file (*.pbtxt)
  as an importerSchemaGen and returns it"""

  # Use ImportSchemaGen to put the curated schema to ML Metadata
  user_schema_importer = tfx.components.ImportSchemaGen(schema_file=schema_file)

  # Run the component
  context.run(user_schema_importer, enable_cache=False)
  
  if display:
    show_schema(context = context, schema_gen=user_schema_importer)

  return user_schema_importer

