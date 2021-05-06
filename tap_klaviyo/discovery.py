import json
from client import authed_get
from streams import Stream
from sync import get_all_pages
from tap_klaviyo import ENDPOINTS, EVENT_MAPPINGS

# GLOBAL_EXCLUSIONS = Stream(stream='global_exclusions', schema=get_schema_from_api_call("global_exclusions", ENDPOINTS["global_exclusions"], api_key), tap_stream_id='global_exclusions', key_properties='email', replication_method='FULL_TABLE')
# LISTS = Stream(stream='lists', schema=None, tap_stream_id='lists', key_properties='id', replication_method='FULL_TABLE')
# CAMPAIGNS = Stream(stream='campaigns', schema=None, tap_stream_id='campaigns', key_properties='id', replication_method='FULL_TABLE')
# FULL_STREAMS = [GLOBAL_EXCLUSIONS, LISTS, CAMPAIGNS]

def discover(api_key):
    metric_streams = get_available_metrics(api_key) # might want to do a simpler version for sync w/o catalog file (or just crash if no catalog)
    global_exclusions = Stream(stream='global_exclusions', schema=get_schema_from_api_call("global_exclusions", ENDPOINTS["global_exclusions"], api_key), tap_stream_id='global_exclusions', key_properties='email', replication_method='FULL_TABLE')
    lists = Stream(stream='lists', schema=get_schema_from_api_call("lists", ENDPOINTS["lists"], api_key), tap_stream_id='lists', key_properties='id', replication_method='FULL_TABLE')
    campaigns = Stream(stream='campaigns', schema=get_schema_from_api_call("campaigns", ENDPOINTS["campaigns"], api_key), tap_stream_id='campaigns', key_properties='id', replication_method='FULL_TABLE')
    full_streams = [global_exclusions, lists, campaigns]
    return {"streams": [a.to_catalog_dict()
                        for a in metric_streams + full_streams]}


def do_discover(api_key):
    print(json.dumps(discover(api_key), indent=2))


def get_available_metrics(api_key):
    metric_streams = []
    for response in get_all_pages('metric_list',
                                  ENDPOINTS['metrics'], api_key):
        for metric in response.json().get('data'):
            schema = get_schema_from_api_call(metric['name'].replace("$", ""), ENDPOINTS['metrics'], api_key)
            metric_streams.append(
                Stream(
                    stream=metric['name'].lower().replace("$", "").replace(" ", "_"),  # we can also remove email, list, to, from, etc
                    schema=schema,
                    tap_stream_id=metric['id'],
                    key_properties="id",
                    replication_method='INCREMENTAL'
                )
            )
    return metric_streams


def get_schema_from_api_call(stream, endpoint, api_key):

    def parse_json_schema(data):
        json_schema = {}
        if isinstance(data, dict):
            json_schema["type"] = "object"
            json_schema["properties"] = {}
            for k, v in data.items():
                # TODO: make sure we don't need to parse out a field named "object"
                json_schema["properties"][k] = parse_json_schema(v)
            return json_schema
        elif isinstance(data, list):
            json_schema["type"] = "array"
            json_schema["properties"] = [parse_json_schema(field) for field in data]
            return json_schema
        else:
            if isinstance(data, str):
                return {"type": "string"}
            elif isinstance(data, int):
                return {"type": "number"}
            elif isinstance(data, int):
                return {"type": "float"}
            elif isinstance(data, bool):
                return {"type": "boolean"}
            # how to see if string is a date, datetime, or timestamp?
            # else:
            #     return {"type": "null"}


    rsp = authed_get(stream, endpoint, {'page': 1, 'api_key': api_key})
    schema = {}
    for i in json.loads(rsp.text).get("data"):
        if i:
            ns = parse_json_schema(i)
            schema.update(ns)
    return schema