import json
import datetime
from dateutil.parser import parse
from client import authed_get
from streams import Stream
from sync import get_all_pages
from tap_klaviyo import ENDPOINTS


def discover(api_key):
    metric_streams = get_available_metrics(api_key)  # might want to do a simpler version for sync w/o catalog file (or just crash if no catalog)
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
    for response in get_all_pages('metric_list', ENDPOINTS['metrics'], api_key):
        for metric in response.json().get('data'):
            schema = get_schema_from_api_call(metric['name'].replace("$", ""), ENDPOINTS['metrics'], api_key, metric['id'])
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


def get_schema_from_api_call(stream, endpoint, api_key, klavio_endpoint_id=None):

    def parse_json_schema(data):
        json_schema = {}
        if isinstance(data, dict):
            json_schema["type"] = "object"
            json_schema["properties"] = {}
            for k, v in data.items():
                json_schema["properties"][k] = parse_json_schema(v)
            return json_schema
        elif isinstance(data, list):
            json_schema["type"] = "array"
            json_schema["items"] = parse_json_schema(max(data, key=len)) \
                                    if data and (isinstance(data[0], dict) or isinstance(data[0], list)) \
                                    else parse_json_schema(data[0]) if data else {}
            return json_schema
        else: # maybe use if it ends with _at it's a timestamp and if it ends with _id it's a string, otherwise go w the python data type
            if isinstance(data, str):  # falsely categorizes amount as a string or a date-time
                try:
                    p = parse(data, fuzzy=False)
                    if isinstance(p, datetime.datetime):
                        return {"type": "string", "format": "date-time"}
                    elif isinstance(p, datetime.date):
                        return {"type": "string", "format": "date"}
                    elif isinstance(p, datetime.time):
                        return {"type": "string", "format": "time"}
                except:
                    return {"type": "string"}
            elif isinstance(data, int):
                return {"type": "integer"}
            elif isinstance(data, float):
                return {"type": "number"}
            elif isinstance(data, bool):
                return {"type": "boolean"}
            else:
                return {"type": "null"}

    def merge_schemas(schema_truth, schema_new):
        schema = schema_truth.copy() if schema_truth else schema_new.copy()
        pd = {"string": 1, "number": 2, "float": 3, "boolean": 3, "object": -1, "array": 0, "null": 1836}
        if schema_truth and len(schema) == 2 and schema.get("type") and schema.get("properties"):
            if schema_truth["type"] != schema_new["type"]:
                if pd[schema_truth["type"]] > pd[schema_new["type"]]: schema[schema_truth]["type"] = schema_new["type"]  # not tested (no hit)
                else: pass
            schema["properties"] = merge_schemas(schema_truth["properties"], schema_new["properties"])
        else:
            if len(schema_truth) == 1 and len(schema_new) == 1 and schema_truth.get("type"):
                if pd[schema_truth["type"]] > pd[schema_new.get("type", "null")]:
                    schema[schema_truth]["type"] = schema_new.get["type"]
            else:
                for field, info in schema_truth.items():
                    for field_2, info_2 in schema_new.items():
                        if field == field_2:
                            if info.get("properties") or info_2.get("properties"):
                                if not info.get("properties"):
                                    schema[field]["properties"] = schema_new[field]["properties"]
                                elif info_2.get("properties"):
                                    schema[field]["properties"] = merge_schemas(info["properties"], info_2["properties"])
                            if info.get("items") or info_2.get("items"):
                                if not info.get("items"):
                                    schema[field]["items"] = schema_new[field]["items"]
                                elif info_2.get("items"):
                                    schema[field]["items"] = merge_schemas(info["items"], info_2["items"])
                            if info["type"] != info_2["type"]:
                                if pd[info["type"]] > pd[info_2["type"]]:
                                    schema[field]["type"] = info_2["type"]
                                else:
                                    pass  # if ==, no change; if schema < schema_2, no change
        return schema

    url = f"{endpoint.replace('metrics', 'metric')}/{klavio_endpoint_id}/timeline" if klavio_endpoint_id else endpoint
    r = authed_get(stream, url, {'api_key': api_key, 'count': 50, 'sort': 'desc'})
    schema = {}
    for i in json.loads(r.text).get("data"):
        if i:
            ns = parse_json_schema(i)
            schema = merge_schemas(schema, ns)
    return schema