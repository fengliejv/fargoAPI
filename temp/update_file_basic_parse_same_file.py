
from minio.commonconfig import CopySource

from TimeJob.embedding.sync_embedding import GALPHA_PARSING_VERSION
from lib.Common.my_minio import Bucket
from service.FileBasicService import check_file_basic_same_file_parsed, set_file_basic_attr, get_file_basic_need_parse
from service.ReportService import add_error_log

if __name__ == '__main__':
    
    files = get_file_basic_need_parse(start_time='2024-01-01 00:00:00', source='gs')
    print(len(files))
    for file in files:
        try:
            same_file = check_file_basic_same_file_parsed(article_id=file['article_id'], source=file['source'],
                                                          start_time='2024-01-01 00:00:00')
            if len(same_file) > 0:
                print(f"{file['uuid']}")
                same_file = same_file[0]
                minio_obj = Bucket()
                minio_obj.client.copy_object(
                    "report-parse-result",
                    f"{file['uuid']}_{GALPHA_PARSING_VERSION}",
                    CopySource("report-parse-result", f"{same_file['uuid']}_{GALPHA_PARSING_VERSION}"),
                )
                del minio_obj
                set_file_basic_attr(file['uuid'], 'parse_status', 'parse_ok')
                print(f"success{file['uuid']}")
                continue
        except Exception as e:
            add_error_log(f"copy{file['uuid']}_{GALPHA_PARSING_VERSION} object fail {e}")
