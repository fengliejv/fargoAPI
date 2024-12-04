
import requests
import time
import json



url = "https://mp.weixin.qq.com/cgi-bin/appmsg"


headers = {
  "Cookie": "appmsglist_action_3927742969=card; RK=qVs19NLiz7; ptcz=47b29beac4ffe39b61fbc571b820d911f728e483a7a9e20df813fa929741df56; rewardsn=; wxtokenkey=777; _qpsvr_localtk=0.021965770866085643; ua_id=yfCkTjkjTa9xMPwgAAAAAD0YLS420VmUankOrhh7Me0=; wxuin=23514589234204; _clck=10vlw8o|1|foa|0; xid=77f1426a807cbc5a7c636d117886c860; mm_lang=zh_CN; uuid=9128d4a7aa82ae16ca9874aeff23150a; cert=V24h90JpTS2wwpOlTcWeSFtO0ouWrJau; uin=o3038722642; sig=h01de2ce86d87fb26e46acd04c2f5456acf07dcf375360d8dc08b3f986fe451d26dac836937ba787811; data_bizuin=3927742969; bizuin=3927742969; master_user=gh_ba5c1ef84c52; master_sid=YmRFRzF4NVJSV09OZ1FBUnVScWx1ZGFFTEFsNlNtVnluNkh3SEw2UVFzbDNYVzk5ajNYeUZZV0NsOVZ0RUNPbThOdHlYeTF4OUZYNFlHQmRsc2hqX21oWXhxU3ZOcjJGVlI2Uk8welJzdFd3b3QyT3FVZjdrbUZyaHA0Z0R4V0dWc2tlcmNZZlU1SXJ2bXZX; master_ticket=0418433c44beb4bf76f1258f70b6f650; data_ticket=XLWxbO6Aye3qw9YD4s8T3fqEhPi2L0a+qIIlN3GGo66yzYYaQ7xyiCuypIjEo4yU; rand_info=CAESIAcXhiYF1jwk9zior+rf/j9o/9FTfAa+5g8glwO/BT5B; slave_bizuin=3927742969; slave_user=gh_ba5c1ef84c52; slave_sid=Z09QMEVhXzBwbGl4WjQ2MEpRZGs3VVpHVFBUb01OdlZzRXM4c2ZERWpaZlk3RlVIZ3ViUGY2VVQ5ZjZ3MVRwZ1VlQ2NRVjQzOWZ5bUdjWFgxQzVtdXB2Ym1vNElhcFVMUlhCSWpNVWhTY3YwNndraExSeHgwRXBUUnU1UXYzYlpTY2xEZTVmRndURlptZHFk; _clsk=1wgucbu|1723530786873|3|1|mp.weixin.qq.com/weheat-agent/payload/record",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
}

"""
需要提交的data
以下个别字段是否一定需要还未验证。
注意修改yourtoken,number
number表示从第number页开始爬取，为5的倍数，从0开始。如0、5、10……
token可以使用Chrome自带的工具进行获取
fakeid是公众号独一无二的一个id，等同于后面的__biz
"""
data = {
    "token": "1565541371",
    "lang": "zh_CN",
    "f": "json",
    "ajax": "1",
    "action": "list_ex",
    "begin": 5,
    "count": "5",
    "query": "",
    "fakeid": "Mzg4MTUyNzI1Mg==",
    "type": "9",
}


content_json = requests.get(url, headers=headers, params=data).json()

for item in content_json["app_msg_list"]:
    
    print(item["title"])
    print(item["link"])