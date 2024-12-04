import os
import sys

sys.path = ['', '/usr/lib/python310.zip', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload',
            '/home/ibagents/.virtualenvs/pythonProject1/lib/python3.10/site-packages',
            '/home/ibagents/.local/lib/python3.10/site-packages', '/usr/local/lib/python3.10/dist-packages',
            '/usr/lib/python3/dist-packages']
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from TimeJob.embedding.galpha.sync_parsing import sync_parsing
from TimeJob.parse.parse_ms_file_to_file_basic import parse_ms_file_to_file_basic
from TimeJob.scrap.ms_file_get import get_ms_report

if __name__ == '__main__':
    get_ms_report()
    parse_ms_file_to_file_basic()
    sync_parsing("ms")
