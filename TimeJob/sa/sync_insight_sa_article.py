import requests
import json
import re

CLIENT_ID = 'cioinsight-backend'
CLIENT_SECRET = '0258d90f-fa98-4b16-916c-51c8a38c3a46'
ARTICLE_TABLE = 'native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad'
FARGO_INSIGHT_KEY = '2e5bbe02-1e66-472e-937c-8d2ded7b4314'
API_TOKEN = ''


def extract_first_stock_code(input_string):
    res = ''
    regex = r'\((.*?)\)'
    matches = re.findall(regex, input_string)
    if not matches:
        matches = re.findall(r'\（(.*?)\）', input_string)
    if matches and 'US' in matches[0]:
        if '/' in matches[0]:
            res = matches[0].split('/')[0]
        else:
            res = matches[0]
        return str(res)
    if matches and 'HK' in matches[0]:
        if '/' in matches[0]:
            res = matches[0].split('/')[0]
        else:
            res = matches[0]
        res = res.split('.HK')[0].rjust(4, '0') + '.HK'
    return str(res)


def get_api_token():
    
    url = "https://auth.easyview.com.hk/realms/Easyview-News-Platform-Realm/protocol/openid-connect/token"
    payload = f"client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&grant_type=client_credentials"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "content-type": "application/x-www-form-urlencoded"
    }
    res = requests.request("POST", url, data=payload, headers=headers)
    if res.status_code != 200:
        return 'fail'
    return json.loads(res.text)['access_token']


def update():
    update_count = 0
    new_count = 0
    newest_id = 0
    article_newest_id = 0
    try:
        
        API_TOKEN = get_api_token()
        
        r = requests.post(
            "https://api.glideapp.io/api/function/queryTables",
            headers={"Authorization": f'Bearer {FARGO_INSIGHT_KEY}'},
            json={
                "appID":
                    "uNOgjdbeolykCXHBMvi0",
                "queries": [{
                    "sql":
                        f"select * from \"{ARTICLE_TABLE}\" where \"x0Y0O\"!=4 and \"m9BiN\"='NP'",
                }]
            })
        rows = json.loads(r.text)[0]['rows']
        for row in rows:
            if len(row) < 3:
                continue
            
            url = f'https://news-platform.easyview.com.hk/api/v1/channel/fargoApp/article/{row["n99aj"]}'
            headers = {"Authorization": f'Bearer {API_TOKEN}'}
            article_res = requests.request("GET", url, headers=headers)
            if article_res.status_code != 200:
                print(f'fail {row["n99aj"]}')
                continue
            
            d = json.loads(article_res.text)
            lang = list(d['titles'].keys())[0]
            if "zh_CN" in list(d['titles'].keys()):
                lang = "zh_CN"
            r = requests.post(
                "https://api.glideapp.io/api/function/mutateTables",
                headers={"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"},
                json={
                    "appID":
                        "uNOgjdbeolykCXHBMvi0",
                    "mutations": [{
                        "kind": "set-columns-in-row",
                        "tableName": f"{ARTICLE_TABLE}",
                        "columnValues": {
                            "n99aj": d['id'],
                            "Jww8r": d['titles'][lang],
                            "wG4ZB": extract_first_stock_code(d['titles'][lang]),
                            "zpVLQ": d['contents'][lang],
                            "ClA6T": d["author"],
                            "ZicAt": d['summaries'][lang],
                            "LppoE": d['metadata']['audit']["publishTime"],
                            "x0Y0O": d['metadata']['workflow']["status"],
                            "s4bkV": d['covers'][lang],
                            "t5GoA": d['thumbnails'][lang],
                            "oCPF0": lang,
                            "ijKmw": json.dumps(d['metadata']['stocks'])
                        },
                        "rowID": row['$rowID']
                    }]
                })
            if r.status_code != 200:
                print(f'fail {row["n99aj"]}')
            update_count += 1

        

        r = requests.post(
            "https://api.glideapp.io/api/function/queryTables",
            headers={"Authorization": f'Bearer {FARGO_INSIGHT_KEY}'},
            json={
                "appID":
                    "uNOgjdbeolykCXHBMvi0",
                "queries": [{
                    "sql":
                        f"SELECT * from \"{ARTICLE_TABLE}\" where \"m9BiN\"='NP' order by \"n99aj\" desc limit 2 "
                }]
            })
        if len(json.loads(r.text)[0]['rows']) > 1:
            newest_id = json.loads(r.text)[0]['rows'][1]['n99aj']
        else:
            return f'fail {r.text}'
        article_newest_id = newest_id
        url = f'https://news-platform.easyview.com.hk/api/v1/channel/fargoApp/articles?articleId.gt={newest_id}'
        headers = {"Authorization": f'Bearer {API_TOKEN}'}
        docs = requests.request("GET", url, headers=headers)
        if docs.status_code != 200:
            return f'fail {newest_id}'
        docs = json.loads(docs.text)['articles']
        for doc in docs:
            
            r = requests.post(
                "https://api.glideapp.io/api/function/queryTables",
                headers={"Authorization": f'Bearer {FARGO_INSIGHT_KEY}'},
                json={
                    "appID":
                        "uNOgjdbeolykCXHBMvi0",
                    "queries": [{
                        "sql":
                            f"SELECT * from \"native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad\" where \"n99aj\"={doc['id']} and \"m9BiN\"='NP'"
                    }]
                })
            if len(json.loads(r.text)[0]['rows']) > 0:
                continue
            lang = list(doc['titles'].keys())[0]
            if "zh_CN" in list(doc['titles'].keys()):
                lang = "zh_CN"
            h = {"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"}
            j = {
                "appID":
                    "uNOgjdbeolykCXHBMvi0",
                "mutations": [{
                    "kind": "add-row-to-table",
                    "tableName": f"{ARTICLE_TABLE}",
                    "columnValues": {
                        "n99aj": doc['id'],
                        "m9BiN": "NP",
                        "wG4ZB": extract_first_stock_code(doc['titles'][lang]),
                        "Jww8r": doc['titles'][lang],
                        "zpVLQ": doc['contents'][lang],
                        "ClA6T": doc["author"],
                        "ZicAt": doc['summaries'][lang],
                        "LppoE": doc['metadata']['audit']["publishTime"],
                        "x0Y0O": doc['metadata']['workflow']["status"],
                        "s4bkV": doc['covers'][lang],
                        "t5GoA": doc['thumbnails'][lang],
                        "oCPF0": lang,
                        "ijKmw": json.dumps(doc['metadata']['stocks'])
                    }
                }]
            }
            r = requests.post("https://api.glideapp.io/api/function/mutateTables",
                              headers=h,
                              json=j)
            if r.status_code != 200:
                pass
            new_count += 1
            print(f'插入新报告成功{doc["id"]}')
            if len(docs) > 0:
                article_newest_id = docs[len(docs) - 1]["id"]
    except Exception as e:
        print(e)
    return f'insight更新前id:{newest_id},文章库最新id:{article_newest_id},更新{update_count}条，新增{new_count}条。'



def update_all():
    update_count = 0
    new_count = 0
    try:
        API_TOKEN = get_api_token()
        
        r = requests.post(
            "https://api.glideapp.io/api/function/queryTables",
            headers={"Authorization": f'Bearer {FARGO_INSIGHT_KEY}'},
            json={
                "appID":
                    "uNOgjdbeolykCXHBMvi0",
                "queries": [{
                    "sql":
                        f'select * from \"{ARTICLE_TABLE}\" where \"x0Y0O\"!=4',
                }]
            })
        rows = json.loads(r.text)[0]['rows']
        for row in rows:
            print(row)
            if len(row) == 0:
                continue
            
            url = f'https://news-platform.easyview.com.hk/api/v1/channel/fargoApp/article/{row["n99aj"]}'
            headers = {"Authorization": f'Bearer {API_TOKEN}'}
            article_res = requests.request("GET", url, headers=headers)
            if article_res.status_code != 200:
                continue
            
            d = json.loads(article_res.text)
            lang = list(d['titles'].keys())[0]
            if "zh_CN" in list(d['titles'].keys()):
                lang = "zh_CN"
            print(lang)
            r = requests.post(
                "https://api.glideapp.io/api/function/mutateTables",
                headers={"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"},
                json={
                    "appID":
                        "uNOgjdbeolykCXHBMvi0",
                    "mutations": [{
                        "kind": "set-columns-in-row",
                        "tableName": f"{ARTICLE_TABLE}",
                        "columnValues": {
                            "n99aj": d['id'],
                            "Jww8r": d['titles'][lang],
                            "zpVLQ": d['contents'][lang],
                            "ClA6T": d["author"],
                            "ZicAt": d['summaries'][lang],
                            "LppoE": d['metadata']['audit']["publishTime"],
                            "x0Y0O": d['metadata']['workflow']["status"],
                            "s4bkV": d['covers'][lang],
                            "t5GoA": d['thumbnails'][lang],
                            "oCPF0": lang,
                            "ijKmw": json.dumps(d['metadata']['stocks'])
                        },
                        "rowID": row['$rowID']
                    }]
                })
            if r.status_code != 200:
                print(f'更新报告失败{row["n99aj"]}')
            update_count += 1
        
        url = f'https://news-platform.easyview.com.hk/api/v1/channel/fargoApp/articles?page=1&pageSize=1&sortBy=articleId&sortOrder=desc&thenOrder=desc'
        headers = {"Authorization": f'Bearer {API_TOKEN}'}
        res = requests.request("GET", url, headers=headers)
        remote_newest_id = json.loads(res.text)['articles'][0]['id']
        print(remote_newest_id)
        
        newest_id = 25600
        
        
        
        
        
        
        
        
        
        
        
        
        
        while (newest_id < remote_newest_id):
            print(newest_id)
            url = f'https://news-platform.easyview.com.hk/api/v1/channel/fargoApp/articles?articleId.gt={newest_id}'
            headers = {"Authorization": f'Bearer {API_TOKEN}'}
            docs = requests.request("GET", url, headers=headers)
            if docs.status_code != 200:
                API_TOKEN = get_api_token()
                continue
            docs = json.loads(docs.text)['articles']
            newest_id = docs[len(docs) - 1]['id']
            print(newest_id)
            for doc in docs:
                if len(doc) == 0:
                    continue
                print(f'开始插入{doc["id"]}')
                
                r = requests.post(
                    "https://api.glideapp.io/api/function/queryTables",
                    headers={"Authorization": f'Bearer {FARGO_INSIGHT_KEY}'},
                    json={
                        "appID":
                            "uNOgjdbeolykCXHBMvi0",
                        "queries": [{
                            "sql":
                                f"SELECT * from \"native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad\" where \"n99aj\"={doc['id']}"
                        }]
                    })
                if len(json.loads(r.text)[0]['rows']) > 0:
                    continue
                lang = list(doc['titles'].keys())[0]
                if "zh_CN" in list(doc['titles'].keys()):
                    lang = "zh_CN"
                r = requests.post(
                    "https://api.glideapp.io/api/function/mutateTables",
                    headers={"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"},
                    json={
                        "appID":
                            "uNOgjdbeolykCXHBMvi0",
                        "mutations": [{
                            "kind": "add-row-to-table",
                            "tableName": f"{ARTICLE_TABLE}",
                            "columnValues": {
                                "n99aj": doc['id'],
                                "Jww8r": doc['titles'][lang],
                                "zpVLQ": doc['contents'][lang],
                                "ClA6T": doc["author"],
                                "ZicAt": doc['summaries'][lang],
                                "LppoE": doc['metadata']['audit']["publishTime"],
                                "x0Y0O": doc['metadata']['workflow']["status"],
                                "s4bkV": doc['covers'][lang],
                                "t5GoA": doc['thumbnails'][lang],
                                "oCPF0": lang,
                                "ijKmw": json.dumps(doc['metadata']['stocks'])
                            }
                        }]
                    })
                if r.status_code != 200:
                    print(f'插入新报告失败{doc["id"]}')

                new_count += 1
                print(f'新增{doc["id"]}')
        return f'更新{update_count}条，新增{new_count}条。'
    except Exception as e:
        print(str(e))
        return str(e)

if __name__ == '__main__':
    update()
