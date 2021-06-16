import singer
from singer import metadata, Transformer, metrics
from tap_klaviyo import ENDPOINTS, EVENT_MAPPINGS, FULL_PULLS
from client import authed_get
from utils import ts_to_dt, dt_to_ts

logger = singer.get_logger()

def update_state(state, entity, dt):
    if dt is None:
        return

    if isinstance(dt, int):
        dt = ts_to_dt(dt)

    if entity not in state:
        state['bookmarks'][entity] = {'since': dt}

    if dt >= state['bookmarks'][entity]['since']:
        state['bookmarks'][entity] = {'since': dt}

    logger.info("Replicated %s up to %s", entity, state['bookmarks'][entity])


def get_starting_point(stream, state, start_date):
    if stream['stream'] in state['bookmarks'] and \
            state['bookmarks'][stream['stream']] is not None:
        return dt_to_ts(state['bookmarks'][stream['stream']]['since'])
    elif start_date:
        return dt_to_ts(start_date)
    else:
        return None

def get_latest_event_time(events):
    return ts_to_dt(int(events[-1]['timestamp'])) if len(events) else None

def get_all_using_next(stream, url, api_key, since=None):
    while True:
        r = authed_get(stream, url, {'api_key': api_key,
                                     'since': since,
                                     'sort': 'asc'})
        yield r
        if 'next' in r.json() and r.json()['next']:
            since = r.json()['next']
        else:
            break


def get_all_pages(source, url, api_key):
    page = 0
    while True:
        r = authed_get(source, url, {'page': page, 'api_key': api_key})
        yield r
        if r.json()['end'] < r.json()['total'] - 1:
            page += 1
        else:
            break

def get_incremental_pull(stream, endpoint, state, api_key, start_date):
    latest_event_time = get_starting_point(stream, state, start_date)

    with metrics.record_counter(stream['stream']) as counter:
        url = '{}{}/timeline'.format(
            endpoint,
            stream['tap_stream_id']
        )
        for response in get_all_using_next(
                stream['stream'], url, api_key,
                latest_event_time):
            events = response.json().get('data')

            if events:
                counter.increment(len(events))
                transfrom_and_write_records(events, stream)
                update_state(state, stream['stream'], get_latest_event_time(events))
                singer.write_state(state)

    return state

def get_full_pulls(resource, endpoint, api_key):

    with metrics.record_counter(resource['stream']) as counter:

        for response in get_all_pages(resource['stream'], endpoint, api_key):

            records = response.json().get('data')
            counter.increment(len(records))
            transfrom_and_write_records(records, resource)


def transfrom_and_write_records(events, stream):
    event_stream = stream['stream']
    event_schema = stream['schema']
    event_mdata = metadata.to_map(stream['metadata'])

    with Transformer() as transformer:
        for event in events:
            singer.write_record(
                event_stream,
                transformer.transform(
                    event, event_schema, event_mdata
                ))

def do_sync(config, state, catalog):
    api_key = config['api_key']
    start_date = config['start_date'] if 'start_date' in config else None

    stream_ids_to_sync = set()

    for stream in catalog.get('streams'):
        mdata = metadata.to_map(stream['metadata'])
        if metadata.get(mdata, (), 'selected'):
            stream_ids_to_sync.add(stream['tap_stream_id'])

    for stream in catalog['streams']:
        if stream['tap_stream_id'] not in stream_ids_to_sync:
            continue
        singer.write_schema(
            stream['stream'],
            stream['schema'],
            stream['key_properties']
        )

        # if stream['stream'] in EVENT_MAPPINGS.values():
        #     get_incremental_pull(stream, ENDPOINTS['metric'], state,
        #                          api_key, start_date)
        # else:
        #     get_full_pulls(stream, ENDPOINTS[stream['stream']], api_key)

        if stream['stream'] in FULL_PULLS:
            get_full_pulls(stream, ENDPOINTS[stream['stream']], api_key)
        else:
            get_incremental_pull(stream, ENDPOINTS['metric'], state,
                                 api_key, start_date)
