import datetime
import json
import os
import sys
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controller.pando import pando_controller
from controller.user import user_controller
from multiprocessing import cpu_count, Process
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request
from gevent import pywsgi
from controller.research import research_controller
from flask_cors import CORS

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=4)  
parsing_file_id = []
LOG_TAG = "sys core api v2"
app.register_blueprint(research_controller)
app.register_blueprint(user_controller)
app.register_blueprint(pando_controller)
CORS(app)


def run_web(multi_process, port):
    if not multi_process:
        pywsgi.WSGIServer(('0.0.0.0', port), app).serve_forever()
    else:
        mulserver = pywsgi.WSGIServer(('0.0.0.0', port), app)
        mulserver.start()

        def server_forever():
            mulserver.start_accepting()
            mulserver._stop_event.wait()

        for i in range(cpu_count() * 1):
            p = Process(target=server_forever)
            p.start()


@app.route('/v2/system/cpu', methods=['GET'])
def get_cpu_count():
    return f"{cpu_count()}"


@app.route('/v2/system/time', methods=['POST'])
def get_time():
    req_data = request.get_json()
    point = req_data.get('point')
    if point=='ping':
        return json.dumps({"result": "pong"})
    current_time = datetime.datetime.now()
    
    formatted_time = current_time.strftime("%Y-%m-%d-%H:%M")
    return json.dumps({"result": f"{formatted_time}"})


if __name__ == "__main__":
    port = 9531
    print(app.url_map)
    if os.path.isfile('/.dockerenv'):
        port = 9530
        
        
        run_web(multi_process=True, port=port)
    else:
        run_web(multi_process=False, port=port)
