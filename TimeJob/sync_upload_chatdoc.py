import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from TimeJob.chatdoc.sync_upload_report import sync_upload_report



if __name__ == '__main__':
    sync_upload_report()
