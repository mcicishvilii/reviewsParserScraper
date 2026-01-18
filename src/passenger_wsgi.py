import os
import sys

# 1. SETUP PATHS FIRST (So Python can find 'dotenv')
VENV_BASE = os.path.join(os.environ['HOME'], 'domains/bookcompare.ladogudi.serv00.net/venv')
VENV_PACKAGES = os.path.join(VENV_BASE, 'lib/python3.12/site-packages')

sys.path.insert(0, VENV_PACKAGES)
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# 2. NOW LOAD DOTENV
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# 3. NOW IMPORT THE APP
from application import application