import datetime
import json
import time
import uuid

from pip._vendor import requests

from lib.Common.files import research_download_get
from service.ReportService import add_fatal_log
from lib.Common.utils import clean_none
from service.ResearchService import get_research_newest, add_research, add_research_attribute

SOURCE = 'jp'
PATH = f"/home/ibagents/files/research/"


def get_jp_research():





    cookies = "mm_rt=.0; TS01f1ccb1=013a94eae2f0f7da825019bc6be6fdfb6275f999682298fe21b95fabbbf4563d74b6c035acd906f2e9978323184001c55295c5f4e4; TS01d3b788=01fd71724b1d159bc49d3a9ae172921660727e21f9696547c9fda61006d2e2d6b4fd58bf1d35da8c8c04c95b89c2dcd5c402f74aea; JSESSIONID=58B714D89C981ECEAF12F08E04C8217B; ut=fd23c910.61e0f97e0cd63; jpmm_bluecoat=1; hart=blue; ppwaf_4959=!vFb7f1x8E9q3KfY9yVlX3KTTQl1Y+b8PNVwCMJK4Gv3qloWa4jG7jAmyUt5cREsWuK8CW8M9RRuIJ1I=; st_aa.publisher0210=.0; plg_rt=.b; X-Global-ID=a48dfa59-763b-40a9-a154-1278ea35d064; st_ms.markets9222=.0; TS019dbfb3=013a94eae2f0f7da825019bc6be6fdfb6275f999682298fe21b95fabbbf4563d74b6c035acd906f2e9978323184001c55295c5f4e4; AMCVS_BDA71C8B5330AE0C0A490D4D%40AdobeOrg=1; AMCV_BDA71C8B5330AE0C0A490D4D%40AdobeOrg=179643557%7CMCIDTS%7C19930%7CMCMID%7C62613650241735862074195538156289796582%7CMCAAMLH-1722506986%7C3%7CMCAAMB-1722506986%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1721909386s%7CNONE%7CvVersion%7C5.5.0; s_cc=true; X_JPM_BG=104219; TS014ab10e=01de3a32f5e27e8bf6e312b8a54c1c96132fe069700bae7cd802ad0339dcd32889ff3dc1ff5c919042d128dbd588ae075f4aa5732d; pajpm3=MNklAHQcYOmQAQAAAAAAAAAAAAB0HGDpkAEAAGQAw5/0p2HBHRkKAu6nqrI5PXTobxs=; pajpm6=oTyYEdVTmYmtWHD7mDZjKAdXCw2wEkwjKmP0rJEoAEqiSssrDIto+zlMjNbhQqgpnnadjgliryMU16fE5VvO0zwFKRdblaMq5oLwK4ztzHddIv6zGDTUzxlANcxYwGV0; SC_TOKEN=GFtfCqVve.sJDa3.bOAM5bth5Rbq3zLwAkx.QF7hOYO3qi5Uw1MrSITmrWLawKcoO7902o0vB-PJCE2SbhwJhHESmPJEpa1JHLrSG-6t-pf2CwaYLepR5tmT77v6Z9t3IZbuCw__; SC_ENCRYPTED=null; nwassbc=oTyYEdVTmYmtWHD7mDZjKAdXCw2wEkwjKmP0rJEoAEqiSssrDIto+zlMjNbhQqgpnnadjgliryMU16fE5VvO0zwFKRdblaMq5oLwK4ztzHddIv6zGDTUzxlANcxYwGV0; pajpmchase2=MNklAHQcYOmQAQAAZAABAAAAcCfhpeEA0JZyCRxzd4qYaBVCllRuNSr3FJod80UvcpUbEkoGRfiGGrgAk+nhpSuRZDzy0DgroDeXAvm1CeWi2l8K/Q5K89UMRj3l+B1R6vlMkdd+HdSMy+bdU3Q1H6J6UgY5nOD1bbfx1oA3JWudvX0/hjQbyDDSb+7VmxejD3XgeR6Yd+EUXkFZQB0OiDcQRgPbzcVoma0ltBOTuOKEPD86HxRRPzOgLp6nNAQ28SVOSEc9HNhkwA2u4Ez4ZyE/YANfjsCX6haK63rBvGDZQj+4vMLIOs2yxZNvl6Z6RW4xgIXrOmoWZmWgWp4q3D6UqXEY2jRPL5q5d7k6mAtbKQ==; pajpmchase3=MNklAHQcYOmQAQAAZAACAAAAPb1d6OC4NZ9DjycbwV+495LOL97p0F0+9SXrpW1f95nq2T8oNECFifiRyhqwalrk6UgALPDOTgKuBhZmSnSPSzAzlBEiK5KFOCO3Mxj/eLcRI7yGbxp0txhbybQO11sUlURj4sx57BzJLP/lgaaG1UnUXa2cl53a5SxpWyV/Gf6oagdlFFA0JCXSiNbJaUDW20Sm+oLW0Txkio9pW3ri4ya2en7zx/qxHLAInqdBLlEbUV1nvkZK1S7YQ4l45WQPtCaT5+iquU3tek++itAsJeZ9hKNdG23b5vGyxGUZ1EXeqR4dMWYVWf7uuT6z+PEDkkOF+WfsE6Tih/4wq56sEw==; pajpmchase1=MNklAHQcYOmQAQAAZAAAAAAAGjcaLLFqs1qPXwCnnP4fUtQUb0IBOPl7Xe44pf/BEf2Trof0K2NJP8GMPXSDYqCBrVIe8etSLGbfziTqXo/S7Xfv+37FU/YcInqF7WVZzTMQ4L2QS0KeVcVZ6h1N53baSfSoufPl8+0jza3IcTLu2ISsO1GbDBoQk9kTmxvtXoTwjId4Mya3zEz/MplUm5x9+syOliU0Vt/YhV6E+F6B/EOhUobBGzw4L4kU3eJGXtLgvJPR/GLqKvfFYS+RZP07VeMxqk9EQ4zAHxgJniG5rcoyvLASP43LyGSHg9QzS3iK89/i/t+qXM4bqbmN1/L3c6ScJ3tQihg3JnAeNo5zuQ==; pajpmlast=MNklAHQcYOmQAQAAAAAAAAAAAAC4AmY3pCKgGg48lbfZFAnhh3RgfQ==; pajpmchase6=oTyYEdVTmYmtWHD7mDZjKCq5JYa4tMDKSL7Ixhmz8ALicsArAPrjftGic2DZ8DD4uEpbzkxCDVu5fNXNy6lSRB5ff0SA1N/K33fYbluJ+TyCs9Ahq8ocoBPkOhS4wBdEynRxPHBUrgV2L3PCg6euKnVAIFn4MKkn/7FPfLDC/5CaJPgGLxM7J9z4645buwc/gMvn6WAbYWwkO6DsrAs2PnTdHF3t28ccOjqus3xkI15eClMgiu0B8WD/GjoeZ+M5rs++E/+eloZa0lCUomXIVTt/gXsZVSGHxuvvs/B+N049RsibDvY3XJvjjPpJ4lrH0ZgSvGw2Jaq98xk/tsfjUWtoZttX3z0co3VOKFxt/pmNoQuUy0m6Dx0mA95b1x5Y5gmT1z/YvzNTl7EVikh3J7Zc0Ojn6zSpRhlicSpZaXg=; TS01c4f269=01de3a32f5e27e8bf6e312b8a54c1c96132fe069700bae7cd802ad0339dcd32889ff3dc1ff5c919042d128dbd588ae075f4aa5732d; JCU_SEL=.a; jpmm=72affcb2-8af3-4a1e-9209-a57c621b4ddd; ua=accepted; JCW_SEL=.b; webutils_rt=.0; jpmm_test_cookie=1721902320455; st_i.Search_Suggestion_Tool3170=.0; s_fid=3C2EC69EAD454E5D-0285821990188857; s_sq=%5B%5BB%5D%5D; st_c.search_advanced_api5991=.0; TS0113fcd4=01fd71724b1d159bc49d3a9ae172921660727e21f9696547c9fda61006d2e2d6b4fd58bf1d35da8c8c04c95b89c2dcd5c402f74aea; TS01cdcdad=01fd71724b1d159bc49d3a9ae172921660727e21f9696547c9fda61006d2e2d6b4fd58bf1d35da8c8c04c95b89c2dcd5c402f74aea; TS01119c60=01fd71724b22eb654ca6b7a580603a28b2199da630bf5c0b91f7f500c0ce37d5a90ba97f4c2cded1ce8b2e9a62a63ee39f880c6b67; __VCAP_ID__=561c8578-7f4d-45c3-5e38-8aac; tabs=1-2; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Jul+26+2024+13%3A58%3A57+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202305.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3ae5ba08-e0d0-466c-ab9c-c2051dd5977d&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CBG249%3A1%2CC0003%3A1%2CC0009%3A1%2CBG250%3A1%2CC0004%3A1%2CC0011%3A1&AwaitingReconsent=false; pajpm1=MNklAP1Dn+2QAQAA5i8JAAAAAAB0HGDpkAEAAGQAeFEUWKuyjlD1y5/Qa5pxF20xFaI=; pajpm2=MNklAP1Dn+2QAQAA5i8JAAAAAAB0HGDpkAEAAGQAjh9qCZpyFY9zdsvTaqUleT+wyvI=; ppnet_4959=!3jpL/Kmkbfz7p6adTz99DXJbZmPRnKqHwOO0GbCcD8ftF95kS5xvl5cY5/bj68mXNT4Vw+GMofq8"
    get_from_search(cookies)


def get_from_search(cookies):
    newest_research = get_research_newest(platform=SOURCE)
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    if len(newest_research) <= 0:
        newest_time = yesterday
    else:
        newest_time = newest_research[0]['publish_time']
    print(newest_time)
    newest_timestamp = int(newest_time.timestamp()) * 1000
    tomorrow = today + datetime.timedelta(days=1)
    start_str = yesterday.strftime('%d/%m/%Y')
    tomorrow_str = tomorrow.strftime('%d/%m/%Y')
    time_range = f'{start_str}..{tomorrow_str}'
    print(time_range)

    url = "https://markets.jpmorgan.com/research/controller/graphql/query-v2"

    payload = {
        "operationName": "research",
        "variables": "{\"businessGroupClassification\":\"ALL\",\"startDate\":\"" + "2022/07/25 16:00" + "\",\"start\":1,\"pageSize\":200,\"exactMatch\":false,\"searchText\":\"\",\"relevanceText\":\"\",\"sortOrder\":\"SCORE\",\"sortDirection\":\"DESCENDING\",\"researchQueryNodeChildren\":[{\"item\":\"SEARCHABLE\",\"operator\":\"EQUALS\",\"values\":\"Y\"},{\"item\":\"AUDIENCE_TYPE_ENTITLEMENT\",\"operator\":\"NOT_EQUALS\",\"values\":\"H-ShareRestricted\"},{\"item\":\"CONTENT_TYPE\",\"operator\":\"NOT_EQUALS\",\"values\":[\"DATAF\",\"ANLYT\",\"MODPT\"]}]}",
        "query": "query research($businessGroupClassification: BusinessGroupClassification, $start: Int, $startDate: String, $endDate: String, $pageSize: Int, $searchText: String, $relevanceText: String, $exactMatch: Boolean, $searchText: String, $sortOrder: ResearchSortOrder, $sortDirection: SortDirection, $researchQueryNodeChildren: [ResearchQueryNode]) {\n  searchService {\n    suggestPages(\n      count: 1\n      queryNode: {operator: AND, children: $researchQueryNodeChildren}\n      text: $searchText\n    ) {\n      id\n      label\n      __typename\n    }\n    __typename\n  }\n  researchService {\n    research(\n      q: {businessGroupClassification: $businessGroupClassification, searchText: $searchText, relevanceText: $relevanceText, endDate: $endDate, startDate: $startDate, options: {inputDateFormat: \"yyyy/MM/dd HH:mm\", start: $start, count: $pageSize, sortOrder: $sortOrder, sortDirection: $sortDirection, exactMatch: $exactMatch, highlightOptions: {fragmentCount: 1, fragmentSize: 300, preTag: \"\", postTag: \"\"}, aggregations: [{aggregation: LANGUAGE, count: 2500}, {aggregation: DOCUMENT_TYPE, count: 2500}, {aggregation: ANALYST, count: 2500}, {aggregation: SECTOR, count: 2500}, {aggregation: COMPANY, count: 5500}, {aggregation: ASSET_CLASS, count: 2500}, {aggregation: BUSINESS_GROUP, count: 2500}, {aggregation: REGION, count: 2500}, {aggregation: COUNTRY, count: 2500}]}, queryNode: {operator: OR, children: [{operator: AND, children: $researchQueryNodeChildren}]}}\n    ) {\n      count\n      aggregations {\n        name\n        results {\n          count\n          displayName\n          value\n          __typename\n        }\n        __typename\n      }\n      results {\n        analysts {\n          results {\n            sid\n            displayName\n            primary\n            active\n            publishingAnalyst\n            businessGroup {\n              displayName\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        companies {\n          results {\n            ricCode {\n              ticker\n              exchange\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        businessGroup {\n          displayName\n          __typename\n        }\n        id\n        pageCount\n        contentTypes\n        documentType\n        documentFormats {\n          mimeType\n          __typename\n        }\n        publicationDate\n        subtitle\n        synopsis\n        fragments\n        title\n        videoDetails {\n          mediaId\n          thumbnail(size: LARGE) {\n            width\n            height\n            url\n            type\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
    }
    headers = {
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Origin": "https://markets.jpmorgan.com",
        "Referer": "https://markets.jpmorgan.com/research/CFP?page=advanced_search_page",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "accept": "*/*",
        "content-type": "application/json",
        "csrftoken": "447O-2TQ3-UFV0-VK0M-A78R-RIC8-A76E-Q01M",
        "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Cookie": cookies
    }

    try:
        reSp = requests.request("POST", url, json=payload, headers=headers, timeout=(30.0, 60.0))
        json_data = json.loads(reSp.text)
        documents = json_data['data']['researchService']['research']['results']
        documents.reverse()
        for doc in documents:
            try:
                if not doc['publicationDate']:
                    continue
                publish_time = time.strptime(doc['publicationDate'], "%a %b %d %H:%M:%S %Z %Y")
                doc_publish_time = int(time.mktime(publish_time)) * 1000
                if doc_publish_time <= newest_timestamp:
                    continue
                file_type = None
                source_url = None
                download_status = False

                p_key = f"{uuid.uuid1()}"
                doc['tripitaka_uuid'] = p_key
                if len(doc['documentFormats']) > 1:
                    file_type = doc['documentFormats'][1]['mimeType'].rpartition('/')[-1]
                if doc['id']:
                    source_url = f"https://markets.jpmorgan.com/research/ArticleServlet?doc={doc['id']}&action=print"
                if source_url and file_type == 'pdf':
                    download_status = research_download_get(file_url=source_url, local_file_path=f"{PATH}{p_key}.pdf",
                                                            data_row=payload,
                                                            header=headers)
                if source_url and file_type == 'zip':
                    download_status = research_download_get(file_url=source_url, local_file_path=f"{PATH}{p_key}.zip",
                                                            data_row=payload,
                                                            header=headers)
                if source_url and file_type == 'xls':
                    download_status = research_download_get(file_url=source_url, local_file_path=f"{PATH}{p_key}.xls",
                                                            data_row=payload,
                                                            header=headers)
                create_time = datetime.datetime.now()
                if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                          value=json.dumps(clean_none(doc)), create_time=create_time):
                    add_research(p_key=p_key, publish_time=publish_time, source=SOURCE, title=doc['title'],
                                 download_status=download_status, create_time=create_time,
                                 file_type=file_type, source_url=source_url)

            except Exception as e:
                error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
                print(error)
                continue
    except Exception as e:
        error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        print(error)
        add_fatal_log(e=e)


if __name__ == '__main__':
    get_jp_research()
