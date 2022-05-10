def initialize():
  import sys
  import subprocess
  import pkg_resources
  
  

def is_installed (package):
  # Getting all the list of packages
  installed_packages = {pkg.key for pkg in pkg_resources.working_set}
  return (package in installed_packages)

def install_package (package):
  initialize()
  # implement pip as a subprocess:
  subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
 
def upgrade_package (package):
  initialize()
  # implement pip as a subprocess:
  subprocess.check_call([sys.executable, '-m', 'pip', 'upgrade', package])
  
  
def update_PIP():
  try:
    import colab
    print ('Upgrading PIP to latest version')
    #!pip install --upgrade pip
  except:
    pass
  
def install_TensorFlow_Data_Validation (reload = True):
  print ('Installing TensorFlow Data Validation - Latest')
  #!pip install tensorflow_data_validation
  if reload:
    reload_packages()
  
def install_TensorFlow_Data_ValidationLessThan2 (reload = True):
  print ('Installing TensorFlow Data Validation')
  #!pip install --upgrade 'tensorflow_data_validation[visualization]<2'
  if reload:
    reload_packages()
  
def install_TensorFlow_Transform (reload = True):
  print ('Installing TensorFlow Transform')
  #!pip install -q -U tensorflow_transform
  if reload:
    reload_packages()

def install_TensorFlow_Model_Analysis (reload = True):
  print ('Installing TensorFlow Model Analysis')
  #!pip install tensorflow-model-analysis
  if reload:
    reload_packages()
  
def install_TFX (reload = True):
  print ('Installing TFX Pipelines')
  #!pip install -U tfx
  if reload:
    reload_packages()
  
def install_Fairness_Indicators (reload == true):
  print ('Installing several libraries including TFDV and TFMA')
  #!pip install fairness-indicators
    if reload:
    reload_packages()
  
def reload_packages():
  print ('Reloading Packages')
  import pkg_resources
  import importlib
  importlib.reload(pkg_resources)
  
