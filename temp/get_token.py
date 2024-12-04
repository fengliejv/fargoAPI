import hashlib
import sys
import urllib.parse
from datetime import datetime


def revise_url(url, extra_params=None, excludes=None):
    extra_params = extra_params or {}
    excludes = excludes or []
    main_url, query = urllib.parse.splitquery(url)
    params = urllib.parse.parse_qs(query) if query else {}
    params.update(extra_params)
    keys = list(params.keys())
    keys.sort()
    params_strings = []
    for key in keys:
        if key in excludes:
            continue
        values = params[key]
        if isinstance(values, list):
            values.sort()
            params_strings.extend(["{}={}".format(key, urllib.parse.quote(str(value))) for value in values])
        else:
            params_strings.append("{}={}".format(key, urllib.parse.quote(str(values))))

    return "{}?{}".format(main_url, "&".join(params_strings)) if params_strings else main_url


def generate_timestamp():
    delta = datetime.utcnow() - datetime.utcfromtimestamp(0)
    return int(delta.total_seconds())


def _generate_token(url, app_id, secret_key, extra_params=None, timestamp=None):
    url = revise_url(url, extra_params=extra_params, excludes=["_token", "_timestamp"])
    timestamp_now = timestamp or generate_timestamp()
    source = "{}
    token = hashlib.md5(source.encode()).hexdigest()
    return token


def encode_url(url, app_id, secret_key, params=None, timestamp=None):
    timestamp = timestamp or generate_timestamp()
    token = _generate_token(url, app_id, secret_key, params, timestamp)
    extra_params = {"_timestamp": timestamp, "_token": token}
    extra_params.update(params or {})
    url = revise_url(url, extra_params=extra_params)
    return url


if __name__ == "__main__":
    URL='https://saas.pdflux.com/api/v1/saas/document/c4619a92-c01a-11ee-89e7-00163e028884/html?user=FargoWealth'
    url = encode_url(URL, 'pdflux', 'EQfwGxU6L9m4')
    print(url)
    