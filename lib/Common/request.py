import ast
import time

def send_headers():
    """
    :return: header
    """
    
    micro_time = str(int(round(time.time() * 1000)))
    TIME = ast.literal_eval('{"X-MICRO-TIME": "' + micro_time + '"}')
    TOKEN = ast.literal_eval('{"X-XSRF-TOKEN": ' + '"21303380-a5c7-42d7-a116-3119157e83ad"}')
    Cookie = ast.literal_eval(
        '{"Cookie": ' + '"SESSION=b92b7f27-9ec1-40ab-bfd9-9932fac81c77; XSRF-TOKEN=21303380-a5c7-42d7-a116-3119157e83ad"}')
    HASH = ast.literal_eval('{"X-HMAC-HASH": ' + '"bce214a7a4fbb23ffecfc8b22fb360bbf5b3adaec73281c499644de4d34799ce"}')

    header_tail = {**TIME, **TOKEN, **Cookie, **HASH}

    header_head = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/110.0",
                   "Accept": "application/json, text/plain, */*",
                   "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
                   "Accept-Encoding": "gzip, deflate",
                   "Content-Type": "application/json;charset=utf-8",
                   "X-USER": "AH340123",
                   "Origin": "http://nmgxt.mohrss.gov.cn",
                   "Connection": "keep-alive",
                   "Referer": "http://nmgxt.mohrss.gov.cn/pp/gkauth/core/console/ui/",
                   }

    reQ_headers = {**header_head, **header_tail}
    
    return reQ_headers