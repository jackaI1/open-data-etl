"""Single-user loopback demo; never expose this configuration to the internet."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/metadata.db"
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
# The analytics file is a deliberately local SQLite connection.
PREVENT_UNSAFE_DB_CONNECTIONS = False
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": False}
LOAD_EXAMPLES = False
WEBSERVER_TIMEOUT = 120
SQLLAB_TIMEOUT = 120
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 30}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 30}
ENABLE_PROXY_FIX = False
