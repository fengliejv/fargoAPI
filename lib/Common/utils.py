import datetime
import hashlib
import re
import time

import deepl
import requests
from pymysql.converters import escape_string

import random
import string

from config import Config
from service.SystemService import get_system_variable


def strbin(s):
    if s:
        return f"'{''.join(format(ord(i), '0>8b') for i in s)}'"
    else:
        return "NULL"


def getTimeNode():
    return datetime.datetime.now() - datetime.timedelta(days=Config.DATA_JUMP_DAY_COUNT)


def filterNull(value):
    if value:
        return f"'{escape_string(value)}'"
    else:
        return "NULL"


def sql_filter(sql, max_length=65535):
    dirty_stuff = ["\"", "\\", "/", "'", "(", ")"]
    for stuff in dirty_stuff:
        sql = sql.replace(stuff, "")
    return sql[:max_length]


def generate_random_string(length=4):
    characters = string.ascii_uppercase + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string


def my_translate_text(text, target_lang, source_lang):
    deepl_glossary_id = get_system_variable(key="deepl_glossary_id")
    deepl_key = get_system_variable(key="deepl_key")
    if not (deepl_glossary_id and deepl_key):
        return None
    translator = deepl.Translator(deepl_key[0]["value"])
    return translator.translate_text(text=text, glossary=deepl_glossary_id[0]["value"],
                                     target_lang=target_lang, source_lang=source_lang).text


def generate_hash(file_path, hash_algorithm='sha256'):

    hash_obj = hashlib.new(hash_algorithm)


    with open(file_path, 'rb') as file:

        for chunk in iter(lambda: file.read(4096), b""):
            hash_obj.update(chunk)


    return hash_obj.hexdigest()


def report_resort(content):
    result = {}
    page_nodes = {}
    end = False
    to_remove = []

    for i in content:
        if not content[i]['metadata']:
            to_remove.append(i)
            continue

        if 'location' not in content[i]['metadata']:
            to_remove.append(i)
            continue

        if 'bbox' not in content[i]['metadata']['location']:
            to_remove.append(i)
            continue

        for h in range(0, 4):
            content[i]['metadata']['location']['bbox'][h] = round(content[i]['metadata']['location']['bbox'][h])
    for i in to_remove:
        del content[i]
    for node_key in content:
        for rule in Config.UBS_FILTER_RULES:
            if rule in content[node_key]['data']:
                end = True
                break
        if end: break
        page_id = content[node_key]['metadata']['page_id']
        if page_id in page_nodes:
            page_nodes[page_id][node_key] = content[node_key]
        else:
            page_nodes[page_id] = {}
            page_nodes[page_id][node_key] = content[node_key]
    for page_id in page_nodes:

        left_content = {}
        right_content = {}
        left_result = {}
        right_result = {}
        for node in page_nodes.get(page_id):
            data = page_nodes[page_id][node]
            if data['metadata']['data_type'] == 'figure':
                continue
            if data['metadata']['location']['bbox'][0] < data['metadata']['location']['page_size'][
                0] / 2:
                left_content[node] = data['metadata']['location']['bbox'][1]
            else:
                right_content[node] = data['metadata']['location']['bbox'][1]

        left_content = dict(sorted(left_content.items(), key=lambda item: item[1]))
        right_content = dict(sorted(right_content.items(), key=lambda item: item[1]))

        temp_same = {}
        left_x_node = {}
        right_x_node = {}
        temp_y = 0
        for i in left_content:
            if content[i]['metadata']['location']['bbox'][1] == temp_y:
                temp_same[i] = content[i]['metadata']['location']['bbox'][0]
                continue
            if len(temp_same.keys()) == 0:
                temp_y = content[i]['metadata']['location']['bbox'][1]
                temp_same[i] = content[i]['metadata']['location']['bbox'][0]
                continue
            if not content[i]['metadata']['location']['bbox'][1] == temp_same.values():

                temp_same = dict(sorted(temp_same.items(), key=lambda item: item[1]))
                for h in temp_same:
                    left_x_node[h] = content[h]
                temp_same.clear()
                temp_same[i] = content[i]['metadata']['location']['bbox'][0]
                temp_y = content[i]['metadata']['location']['bbox'][1]

        for i in right_content:
            if content[i]['metadata']['location']['bbox'][1] in temp_same:
                temp_same[i] = content[i]['metadata']['location']['bbox'][0]
            if content[i]['metadata']['location']['bbox'][1] not in temp_same and len(temp_same.keys()) > 0:

                temp_same = dict(sorted(temp_same.items(), key=lambda item: item[1]))
                for h in temp_same:
                    right_x_node[h] = content[h]
                temp_same.clear()
            if len(temp_same.keys()) == 0:
                temp_same[i] = content[i]['metadata']['location']['bbox'][0]

        left_content = left_x_node
        right_content = right_x_node

        for i in left_content:
            if i in left_result:
                continue
            left_result[i] = content[i]
            max_box = left_result[i]['metadata']['location']['bbox']
            for h in left_content:
                if calculate_min_distance(max_box, content[h]['metadata']['location']['bbox']) < 12:
                    left_result[h] = content[h]
                    for z in range(0, 4):
                        max_box[z] = max(content[h]['metadata']['location']['bbox'][z], max_box[z])
                    break
        for i in right_content:
            if i in right_result:
                continue
            right_result[i] = content[i]
            max_box = right_result[i]['metadata']['location']['bbox']
            for h in right_content:
                if calculate_min_distance(max_box, content[h]['metadata']['location']['bbox']) < 15:
                    right_result[h] = content[h]
                    for z in range(0, 4):
                        max_box[z] = max(content[h]['metadata']['location']['bbox'][z], max_box[z])
                    break
        left_result.update(right_result)
        for node in left_result:
            if content[node]['metadata']['data_type'] == 'text':
                content[node]['data'] = content[node]['data'].strip("\n").replace('\n', ' ')
            result[node] = content[node]
    return result


def calculate_min_distance(bbox1, bbox2):



    if bbox1[2] < bbox2[0]:
        dx = bbox2[0] - bbox1[2]
    elif bbox2[2] < bbox1[0]:
        dx = bbox1[0] - bbox2[2]
    else:
        dx = 0


    if bbox1[3] < bbox2[1]:
        dy = bbox2[1] - bbox1[3]
    elif bbox2[3] < bbox1[1]:
        dy = bbox1[1] - bbox2[3]
    else:
        dy = 0



    return (dx ** 2 + dy ** 2) ** 0.5


def add_single_quotes_to_patterns(text):

    datetime_pattern = r'\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b'


    special_pattern = r'(like )%%([^%]+)%%'


    modified_text = re.sub(datetime_pattern, r"'\g<0>'", text)


    modified_text = re.sub(special_pattern, r"\1'%%\2%%'", modified_text)

    return modified_text


def make_request(method, url, headers=None, params=None, retries=5, retry_delay=60):
    for attempt in range(retries):
        try:
            request_args = {
                'method': method,
                'url': url,
                'timeout': retry_delay
            }

            if headers is not None:
                request_args['headers'] = headers

            if params is not None:
                request_args['params'] = params

            response = requests.request(**request_args)
            response.raise_for_status()
            return response
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(retry_delay)
            else:
                raise e


def clean_none(d):
    if isinstance(d, dict):
        return {k: clean_none(v) for k, v in d.items() if v not in [None, '', [], {}, 'null']}
    elif isinstance(d, list):
        return [clean_none(v) for v in d if v not in [None, '', [], {}, 'null']]
    else:
        return d
