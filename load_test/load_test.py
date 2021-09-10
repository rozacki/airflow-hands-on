import requests
from pprint import pprint
import json
import base64

dag_id = 'event_driven_params'
url = f'http://localhost:8080/api/v1/dags/{dag_id}/dagRuns'
body = {
    "conf":
        {"snapshot_id": ""}
}
for i in range(0, 500):
    body['conf']['snapshot_id'] = str(i)

    print(json.dumps(body))
    encoded_pass = base64.b64encode(b'admin:admin').decode()

    r = requests.post(url, data=json.dumps(body), headers={f'Authorization': f'Basic {encoded_pass}',
                                               'Content-type': 'application/json'})
    print(r.content)
    print(r.status_code)