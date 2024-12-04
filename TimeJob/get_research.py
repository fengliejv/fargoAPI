import os
import sys

sys.path = ['', '/usr/lib/python310.zip', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload',
            '/home/ibagents/.virtualenvs/pythonProject1/lib/python3.10/site-packages',
            '/home/ibagents/.local/lib/python3.10/site-packages', '/usr/local/lib/python3.10/dist-packages',
            '/usr/lib/python3/dist-packages', '/usr/bin']
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from TimeJob.scrap.get_sa_research import get_sa_research
from TimeJob.scrap.get_fargo_research import get_fargo_research

if __name__ == '__main__':
    get_sa_research()
    get_fargo_research()
