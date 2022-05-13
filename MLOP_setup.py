import sys
import subprocess as sp
import pkg_resources
import os

PACKAGE_DICT = {'TENSORFLOW':'tensorflow', 
                'TF_DATA_VALIDATION':'tensorflow-data-validation',
                'TF_TRANSFORM':'tensorflow-transform',
                'TFX':'tfx',
                'TF_MODEL_ANALYSIS':'tensorflow-model-analysis',
                'TF_FAIRNESS_INDICATORS':'fairness-indicators',
                'PIP':'pip', 'WGET':'wget'}

LIBRARY_URL = 'https://raw.githubusercontent.com/fernandoGitHub/MLOPS_GSD/main/'

LIBRARY_DICT = {'TF_pipeline':'TF_pipeline.py', 'TF_stat':'TF_stat.py', 'TF_transform':'TF_transform.py'}

def load_and_import_TF_libraries():
  """load_and_import_TF_libraries loads the libraries from the Github repository
  and imports them into the session"""
  
  # Importing wget library
  install_package('WGET')
  import wget
  
  for i, (key, value) in enumerate(LIBRARY_DICT.items()):
    file_name = wget.download(os.path.join(LIBRARY_URL, value))
    print(f"Fetching from GitHub: {file_name} ...")
    __import__(key)
    print(f"Importing {key} ...")
    
    
def display_packages():
  print (PACKAGE_DICT.keys())


def display_installed_packages():
  print (get_installed_packages())


def get_installed_packages():
  return sorted({pkg.key for pkg in pkg_resources.working_set})


def get_package (package_key):
  return PACKAGE_DICT.get (package_key, 'Not Found')


def is_installed (package):
  # Getting all the list of packages
  return (package in get_installed_packages())


def install_package(package_key, reload = True):

  package = get_package(package_key)
  if (package == 'Not Found'):
    print ('Wrong Key! Install will be skipped')
    return

  # Veryfing existing install prior to install
  if is_installed(package):
    print (f'Package: {package} is already installed. Skipping installation')
  else:
    print(f'Installing {package} ...')
    sp.check_call([sys.executable, '-m', 'pip', 'install', package])
    print(f'Package {package} has been successfully installed')

  if reload:
    reload_packages()


def reload_packages():
  print ('Reloading Packages')
  import pkg_resources
  import importlib
  importlib.reload(pkg_resources)
  

  
