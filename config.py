
import os


DEFAULTS = {
    'DB_USERNAME': 'root',
    'DB_PASSWORD': 'Fargowealth135!',
    'DB_HOST': '4.242.20.9',
    'DB_PORT': 9536,
    'DB_DATABASE': 'DB_Insight',
    'DB_CHARSET': 'utf8',
    'CLIENT_ID': 'cioinsight-backend',
    'CLIENT_SECRET': '0258d90f-fa98-4b16-916c-51c8a38c3a46',
    'ARTICLE_TABLE': 'native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad',
    'FARGO_INSIGHT_KEY': '2e5bbe02-1e66-472e-937c-8d2ded7b4314',
    'DATA_START_TIME': '2023-11-07 00:00:00',
    'FILE_STORAGE_PATH': '/home/ibagents/files/',
    'OSS_KEY': 'w5qPjZeiOS5QHNERKHUb',
    'OSS_SECRET': 'cEK8wMyjhsrJ2rxuIHibOFLBjgUYQZjiFbu2ybYF',
    'OSS_HOST': '212.64.23.164:9527',
    'DATA_JUMP_DAY_COUNT': 40,
    'UBS_FILTER_RULES': {"The Disclaimer relevant to Global", "\nDisclosure Section", "\nDisclosure Appendix"}
}


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
    CLIENT_ID = get_env('CLIENT_ID')
    CLIENT_SECRET = get_env('CLIENT_SECRET')
    ARTICLE_TABLE = get_env('ARTICLE_TABLE')
    FARGO_INSIGHT_KEY = get_env('FARGO_INSIGHT_KEY')
    DATA_START_TIME = get_env('DATA_START_TIME')
    FILE_STORAGE_PATH = get_env('FILE_STORAGE_PATH')
    OSS_KEY = get_env("OSS_KEY")
    OSS_SECRET = get_env("OSS_SECRET")
    OSS_HOST = get_env("OSS_HOST")
    DATA_JUMP_DAY_COUNT = get_env('DATA_JUMP_DAY_COUNT')
    UBS_FILTER_RULES = get_env('UBS_FILTER_RULES')
