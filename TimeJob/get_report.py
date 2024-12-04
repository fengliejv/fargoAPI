import os
import sys

sys.path = ['', '/usr/lib/python310.zip', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload',
            '/home/ibagents/.virtualenvs/pythonProject1/lib/python3.10/site-packages',
            '/home/ibagents/.local/lib/python3.10/site-packages', '/usr/local/lib/python3.10/dist-packages',
            '/usr/lib/python3/dist-packages', '/usr/bin']
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from TimeJob.scrap.gs_file_get import get_gs_report
from TimeJob.scrap.ms_file_get import get_ms_report
from TimeJob.scrap.ubs_file_get import get_ubs_report
from TimeJob.scrap.quartr_calendar_get import get_quartr_calendar

if __name__ == '__main__':
    get_ubs_report()
    get_ms_report()
    get_gs_report()
    get_quartr_calendar()
