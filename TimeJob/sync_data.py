import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from TimeJob.content_generate.generate_sa_content_to_summary import generate_sa_content_to_summary
from TimeJob.content_generate.generate_sa_question_to_answer import generate_sa_question_to_answer
from TimeJob.content_generate.generate_sa_summary_to_question import generate_sa_summary_to_question
from TimeJob.insight.sync_sa_article import sync_sa_article
from TimeJob.insight.sync_view_card import sync_view_card
from TimeJob.parse.parse_sa_file_to_file_basic import parse_sa_file_to_file_basic
from TimeJob.parse.parse_sa_file_to_file_basic_view import parse_sa_file_to_file_basic_view
from TimeJob.scrap.sa_file_get import get_sa_report

if __name__ == '__main__':
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    get_sa_report()
    parse_sa_file_to_file_basic()
    parse_sa_file_to_file_basic_view()
    generate_sa_content_to_summary()
    generate_sa_summary_to_question()
    generate_sa_question_to_answer()
    sync_sa_article()
    sync_view_card()
