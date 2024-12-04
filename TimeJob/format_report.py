import os
import sys

sys.path = ['', '/usr/lib/python310.zip', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload',
            '/home/ibagents/.virtualenvs/pythonProject1/lib/python3.10/site-packages',
            '/home/ibagents/.local/lib/python3.10/site-packages', '/usr/local/lib/python3.10/dist-packages',
            '/usr/lib/python3/dist-packages', '/usr/bin']
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from TimeJob.parse.parse_gs_file_to_file_basic import parse_gs_file_to_file_basic
from TimeJob.parse.parse_ms_file_to_file_basic import parse_ms_file_to_file_basic
from TimeJob.parse.parse_ubs_file_to_file_basic import parse_ubs_file_to_file_basic

if __name__ == '__main__':
    parse_gs_file_to_file_basic()
    parse_ubs_file_to_file_basic()
    parse_ms_file_to_file_basic()
