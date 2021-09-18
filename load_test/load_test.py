import requests
import json
import base64

dag_id = 'event_driven_params'
url = f'http://localhost:8080/api/v1/dags/{dag_id}/dagRuns'
body = {
    "conf":
        {"snapshot_id": ""}
}

number_of_steps = 500
encoded_pass = base64.b64encode(b'admin:admin').decode()
headers = {f'Authorization': f'Basic {encoded_pass}', 'Content-type': 'application/json'}

for i in range(0, number_of_steps):
    body['conf']['snapshot_id'] = str(i)
    print(json.dumps(body))
    r = requests.post(url, data=json.dumps(body), headers=headers)
    print(r.content)
    print(r.status_code)