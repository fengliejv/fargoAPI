import csv
import json
import re

from bs4 import BeautifulSoup

from service.ReportService import get_file_record_by_type


def run():
    docs = get_file_record_by_type(type='seekingalpha.com')
    doc_dict = []
    data = []
    for doc in docs:
        id = json.loads(doc['attribute'])['id']
        try:
            if id in doc_dict:
                continue
            else:
                doc_dict.append(id)
            with open(doc['file_path'], 'r', encoding='UTF-8') as f:
                content = f.read()
                f.close()
                responseHtml = BeautifulSoup(content, 'html.parser')
                head = responseHtml.find('img', attrs={'data-test-id': 'user-pic'})
                head = head.get("src")
                name = responseHtml.find('a', attrs={'data-test-id': 'author-name'})
                name = name.text
                viewpoint = re.findall("{([^{}]*sentiment\",\"attributes\".*?)}", content, flags=re.S)
                if len(viewpoint) > 0:
                    viewpoint = str(viewpoint[0])
                    viewpoint = viewpoint[viewpoint.find("{"):len(viewpoint)] + "}"
                    viewpoint = json.loads(viewpoint)['type']
                else:
                    viewpoint = ""
                article_id = json.loads(doc['attribute'])['id']
                title = json.loads(doc['attribute'])['attributes']['title']
                data.append([article_id, title, name, head, viewpoint])
                print(f"处理{id}")
        except Exception as e:
            print(str(e))
        with open('articles_attibute.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)


if __name__ == '__main__':
    run()
