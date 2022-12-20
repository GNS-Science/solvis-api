"""
ref https://pynamodb.readthedocs.io/en/latest/settings.html

Default settings may be overridden by providing a Python module which exports the desired new values.

Set the PYNAMODB_CONFIG environment variable to an absolute path to this module
or write it to /etc/pynamodb/global_default_settings.py to have it automatically discovered.

"""
max_retry_attempts = 10  # default is 3
base_backoff_ms = 100  # default 25

print(f"{__name__} module was loaded")
