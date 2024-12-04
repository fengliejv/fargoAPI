import os
import sys

sys.path = ['', '/usr/lib/python310.zip', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload',
            '/home/ibagents/.virtualenvs/pythonProject1/lib/python3.10/site-packages',
            '/home/ibagents/.local/lib/python3.10/site-packages', '/usr/local/lib/python3.10/dist-packages',
            '/usr/lib/python3/dist-packages']
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from TimeJob.yida.bank_sync import SyncBank
from TimeJob.yida.bank_ltv_sync import SyncBankLtv
from TimeJob.yida.symbol_sync import SyncSymbol

if __name__ == '__main__':
    print(sys.argv)
    if sys.argv[1] == "bank":
        SyncBank.main(sys.argv[1:])
    elif sys.argv[1] == "symbol":
        SyncSymbol.main(sys.argv[1:])
    elif sys.argv[1] == "ltv":
        SyncBankLtv.main(sys.argv[1:])
    else:
        print(sys.argv[1])
