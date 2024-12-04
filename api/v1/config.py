
import os

DEFAULTS = {
    'DB_USERNAME': 'fargoinsight',
    'DB_PASSWORD': 'Fargowealth1357!',
    'DB_HOST': '4.242.20.9',
    'DB_PORT': 9536,
    'DB_DATABASE': 'DB_Insight',
    'DB_CHARSET': 'utf8',
    'OSS_KEY': 'w5qPjZeiOS5QHNERKHUb',
    'OSS_SECRET': 'cEK8wMyjhsrJ2rxuIHibOFLBjgUYQZjiFbu2ybYF',
    'OSS_HOST': '212.64.23.164:9527',
    'UBS_FILTER_RULES': {"The Disclaimer relevant to Global", "\nDisclosure Section", "\nDisclosure Appendix"}
}

if os.path.isfile('/.dockerenv'):
    DEFAULTS['DB_HOST'] = 'host.docker.internal'


def get_env(key):
    return os.environ.get(key, DEFAULTS.get(key))


def get_bool_env(key):
    return get_env(key).lower() == 'true'


def get_cors_allow_origins(env, default):
    cors_allow_origins = []
    if get_env(env):
        for origin in get_env(env).split(','):
            cors_allow_origins.append(origin)
    else:
        cors_allow_origins = [default]

    return cors_allow_origins


class Config:
    """Application configuration class."""

    
    
    
    TESTING = False
    

    
    
    
    DB_USERNAME = get_env("DB_USERNAME")
    DB_PASSWORD = get_env("DB_PASSWORD")
    DB_HOST = get_env("DB_HOST")
    DB_PORT = get_env("DB_PORT")
    DB_DATABASE = get_env("DB_DATABASE")
    DB_CHARSET = get_env("DB_CHARSET")
    OSS_KEY = get_env("OSS_KEY")
    OSS_SECRET = get_env("OSS_SECRET")
    OSS_HOST = get_env("OSS_HOST")
    UBS_FILTER_RULERS = get_env("UBS_FILTER_RULES")
