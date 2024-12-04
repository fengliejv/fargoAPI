import json

from flask import Flask, send_file, make_response
from gevent import pywsgi
import pymysql
import os

DEFAULTS = {
    'DB_USERNAME': 'fargoinsight',
    'DB_PASSWORD': 'Fargowealth1357!',
    'DB_HOST': '4.242.20.9',
    'DB_PORT': 9536,
    'DB_DATABASE': 'DB_Insight',
    'DB_CHARSET': 'utf8',
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
    UBS_FILTER_RULERS = get_env("UBS_FILTER_RULES")


class MySQL(object):
    _instance = None

    @staticmethod
    def get_instance():
        MySQL._instance = MySQL()
        return MySQL._instance

    def __init__(self):
        self.conn = pymysql.connect(host=Config.DB_HOST,
                                    port=Config.DB_PORT,
                                    user=Config.DB_USERNAME,
                                    password=Config.DB_PASSWORD,
                                    db=Config.DB_DATABASE,
                                    charset="utf8")
        self.cursor = self.conn.cursor()


def query(sql, *args):
    """
    :param sql:
    :param args:
    :return: 返回查询到的结果，((),(),)的形式
    """
    obj = MySQL.get_instance()
    obj.cursor.execute(sql, *args)
    res = obj.cursor.fetchall()
    return res


def execute(sql, *args):
    obj = MySQL.get_instance()
    row = obj.cursor.execute(sql, *args)
    obj.conn.commit()
    return row


def query_dict(sql, *args):
    """
    :param sql:
    :param args:
    :return: 返回查询到的结果，dict的形式
    """
    obj = MySQL.get_instance()
    obj.cursor.execute(sql, *args)
    result = obj.cursor.fetchall()
    index_dict = get_index_dict(obj.cursor)
    res = []
    for datai in result:
        resi = dict()
        for indexi in index_dict:
            resi[indexi] = datai[index_dict[indexi]]
        res.append(resi)
    return res


def query_key_dict(sql, *args, key):
    """
    :param sql:
    :param args:
    :return: 返回查询到的结果，key:dict的形式
    """
    obj = MySQL.get_instance()
    obj.cursor.execute(sql, *args)
    result = obj.cursor.fetchall()
    index_dict = get_index_dict(obj.cursor)
    res = {}
    for datai in result:
        resi = dict()
        for indexi in index_dict:
            resi[indexi] = datai[index_dict[indexi]]
        res[resi[key]] = resi
    return res


def get_index_dict(cursor):
    """
    获取数据库对应表中的字段名
    """
    index_dict = dict()
    index = 0
    for desc in cursor.description:
        index_dict[desc[0]] = index
        index = index + 1
    return index_dict


app = Flask(__name__)


@app.route('/report/<source>/<company>/<filename>')
def download_file(source, company, filename):
    headers = ("Content-Disposition",)

    file_path = f'/home/ibagents/files/{source}/{company}/{filename}'
    response = make_response(send_file(file_path, as_attachment=False))
    response.headers["Content-Disposition"] = f"inline;filename={filename}"
    return response


@app.route('/pdf/replace/<filename>')
def pdf_replace_file(filename):
    headers = ("Content-Disposition",)

    file_path = f'/home/ibagents/files/replace_file/{filename}'
    response = make_response(send_file(file_path, as_attachment=False))
    response.headers["Content-Disposition"] = f"inline;filename={filename}"
    return response


@app.route('/file/<file_id>.pdf')
def get_file(file_id):
    try:
        headers = ("Content-Disposition",)
        sql = f"select * from TB_File_Basic where uuid=%s LIMIT 1"
        data = query_dict(sql, (file_id,))
        if len(data) > 0:

            file_path = f"{data[0]['local_save_path']}"
            response = make_response(send_file(file_path, as_attachment=False))
            response.headers["Content-Disposition"] = f"inline;filename={file_id}.pdf"
            return response
        else:
            return "file not found"
    except Exception as e:
        return f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'


@app.route('/file/<file_id>.pdf)')
def get_file_wechat(file_id):
    try:
        headers = ("Content-Disposition",)
        sql = f"select * from TB_File_Basic where uuid=%s LIMIT 1"
        data = query_dict(sql, (file_id,))
        if len(data) > 0:

            file_path = f"{data[0]['local_save_path']}"
            response = make_response(send_file(file_path, as_attachment=False))
            response.headers["Content-Disposition"] = f"inline;filename={file_id}.pdf"
            return response
        else:
            return "file not found"
    except Exception as e:
        return f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'


@app.route('/research/file/<filename>')
def get_research_file(filename):
    try:
        file_path = f"/home/ibagents/files/research/{filename}"
        if os.path.exists(file_path):
            response = make_response(send_file(file_path, as_attachment=False))
            response.headers["Content-Disposition"] = f"inline;filename={filename}"
            return response
        else:
            return "file not found"
    except Exception as e:
        return f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'


@app.route('/static/<platform>/<filename>')
def get_static_file(platform, filename):
    try:
        file_path = f"/home/ibagents/files/static/{platform}/{filename}"
        if os.path.exists(file_path):
            response = make_response(send_file(file_path, as_attachment=False))
            response.headers["Content-Disposition"] = f"inline;filename={filename}"
            return response
        else:
            return "file not found"
    except Exception as e:
        return f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'


@app.route('/files/<file>')
def get_files(file):
    try:
        file_id = file.rsplit('.', 1)[0]
        file_type = file.rsplit('.', 1)[1]
        sql = f"select * from TB_File_Basic where uuid=%s and type=%s LIMIT 1"
        data = query_dict(sql, (file_id, file_type))
        if len(data) > 0:
            if data[0]['source'] == 'quartr_ts' and data[0]['type'] == 'json':
                with open(data[0]['local_save_path'], 'r', encoding='utf-8') as fis:
                    json_data = json.load(fis)
                    return json_data['transcript']['text']
            file_path = f"{data[0]['local_save_path']}"
            response = make_response(send_file(file_path, as_attachment=True))
            response.headers["Content-Disposition"] = f"filename={file}"
            return response
        else:
            return "file not found"
    except Exception as e:
        return f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'


if __name__ == "__main__":
    server = pywsgi.WSGIServer(('0.0.0.0', 9527), app)
    server.serve_forever()
