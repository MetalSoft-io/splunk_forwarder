#!/bin/python

#pip install git+git://github.com/georgestarcher/Splunk-Class-httpevent.git
#pip install metal-cloud-sdk

from splunk_http_event_collector import http_event_collector
import metal_cloud_sdk.clients.api as ms
from jsonrpc2_base.plugins.client.signature_add import SignatureAdd
from jsonrpc2_base.plugins.client.debug_logger import DebugLogger

import os
import time
import datetime
import dateutil.parser

http_event_collector_key = "1cc5ebbd-3efa-43a8-8f5d-7b62d73b5574"
http_event_collector_host = "localhost"

metalsoft_api_key = os.environ["METALCLOUD_API_KEY"]
metalsoft_endpoint = "https://api.bigstep.com/api/developer/developer"

def init_metalsoft_client(apiKey, endpoint):        
        dictParams = {
            "strJSONRPCRouterURL": endpoint
        }

        client = ms.API.getInstance(
            dictParams,
            [
                SignatureAdd(apiKey, {}),
                DebugLogger(True, "DebugLogger.log")
            ]
        )

        return client

def get_events(client, last_event_id, limit_start, limit):
    q='''SELECT event_id, event_type, event_severity, event_visibility, event_title, event_message, event_occurred_timestamp,infrastructure_id,server_id,user_id, afc_id,datacenter_name, event_http_user_agent, user_email_authenticated 
    FROM _events 
    WHERE event_id>{} 
    ORDER BY event_id DESC 
    LIMIT {}, {}'''.format(last_event_id, limit_start, limit)

    print(q)

    return client.query(7, q)




c = init_metalsoft_client(metalsoft_api_key, metalsoft_endpoint)
collector = http_event_collector(http_event_collector_key, http_event_collector_host)


last_event_id=18893137
first_seen_event_id=last_event_id

while True:

    first_batch=True
    limit_start=0
    limit = 1000

    while True:
        res = get_events(c, last_event_id, limit_start, limit)
        
        for evt in res["rows"]:
            payload={}
            payload.update({"index":"main"})
            payload.update({"sourcetype":"_json"})
            payload.update({"source":"metalsoft"})
            
            
            dt = datetime.datetime.strptime(evt["event_occurred_timestamp"], "%Y-%m-%dT%H:%M:%SZ")
            t = time.mktime(dt.timetuple())

            
            payload.update({"time":t})
            payload.update({"timestamp":t})
            payload.update({"host":evt["server_id"]})
            payload.update({"event":evt})
          #  collector.batchEvent(payload)
            collector.sendEvent(payload)
            
      #  collector.flushBatch()
        
        if first_batch and len(res["rows"])>0:
            first_seen_event_id = res["rows"][0]["event_id"]
            first_batch = False

        limit_start += limit

        print(res["rows_total"])
        print(len(res["rows"]))

        if res["rows_total"]<limit_start:
            last_event_id = first_seen_event_id
            break
    
    
    time.sleep(5)