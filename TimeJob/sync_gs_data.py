import os
import sys

sys.path = ['', '/usr/lib/python310.zip', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload',
            '/home/ibagents/.virtualenvs/pythonProject1/lib/python3.10/site-packages',
            '/home/ibagents/.local/lib/python3.10/site-packages', '/usr/local/lib/python3.10/dist-packages',
            '/usr/lib/python3/dist-packages', '/usr/bin']
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from TimeJob.embedding.galpha.sync_gs_parsing import sync_parsing
from TimeJob.parse.parse_gs_file_to_file_basic import parse_gs_file_to_file_basic
from TimeJob.scrap.gs_file_get import get_gs_report

if __name__ == '__main__':
    get_gs_report()
    parse_gs_file_to_file_basic()
    sync_parsing("gs")
