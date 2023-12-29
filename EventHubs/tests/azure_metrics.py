from azure.servicebus import ServiceBusService
from requests import Session
import json
from datetime import datetime
import time
eventhubname = "<eventhubname>"
sharedkey = "<sharedkey>"
sbs = ServiceBusService(
            eventhubname,
            shared_access_key_name="RootManageSharedAccessKey",
            shared_access_key_value=sharedkey,
            request_session=Session()
        )
ex_id = 1
mock_logs = json.load(open('metrics_fixtures.json'))
mock_logs_payload = json.dumps(mock_logs)
mock_logs_payload = mock_logs_payload.replace("2018-03-07T14:23:51.991Z", datetime.utcnow().isoformat())
mock_logs_payload = mock_logs_payload.replace("C088DC46", "%d-%s" % (ex_id, str(int(time.time()))))
#print("inserting %s" % (mock_logs_payload))
sbs.send_event("insights-metrics-pt1m", mock_logs_payload)
print("Event inserted")
