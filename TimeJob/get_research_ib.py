import os
import sys

sys.path = ['', '/usr/lib/python310.zip', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload',
            '/home/ibagents/.virtualenvs/pythonProject1/lib/python3.10/site-packages',
            '/home/ibagents/.local/lib/python3.10/site-packages', '/usr/local/lib/python3.10/dist-packages',
            '/usr/lib/python3/dist-packages', '/usr/bin']
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from TimeJob.scrap.get_gs_research import get_gs_research
from TimeJob.scrap.get_ms_research import get_ms_research
from TimeJob.scrap.get_ubs_research import get_ubs_research
from TimeJob.scrap.get_jp_research import get_jp_research

if __name__ == '__main__':
    get_gs_research()
    get_ubs_research()
    get_jp_research()
    get_ms_research()
