import json
from singer import metadata
from utils import get_abs_path

class Stream(object):
    def __init__(self, stream, schema, tap_stream_id, key_properties, replication_method):
        self.stream = stream
        self.schema = schema
        self.tap_stream_id = tap_stream_id
        self.key_properties = key_properties
        self.replication_method = replication_method
        self.metadata = []

    def to_catalog_dict(self):
        #schema = self.load_schema(self.stream)  # this is not gonna work - lots of stuff missing
        #  schema = self.get_schema_from_api_calls(self.stream, ENDPOINTS['metrics'], api_key)
        mdata = metadata.to_map(
            metadata.get_standard_metadata(
                schema = self.schema,
                key_properties = self.key_properties,
                replication_method = self.replication_method
            )
        )

        if self.replication_method == 'INCREMENTAL':
            mdata = metadata.write(mdata, ('properties', 'timestamp'), 'inclusion', 'automatic')
        self.metadata = metadata.to_list(mdata)

        return {
            'stream': self.stream,
            'tap_stream_id': self.tap_stream_id,
            'key_properties': [self.key_properties],
            'schema': self.schema,
            'metadata': self.metadata
        }

    def load_schema(self, name):
        return json.load(open(get_abs_path('schemas/{}.json'.format(name))))

    # def get_full_pulls(self, resource, endpoint, api_key):
    #
    #     with metrics.record_counter(resource['stream']) as counter:
    #
    #         for response in get_all_pages(resource['stream'], endpoint, api_key):
    #
    #             records = response.json().get('data')
    #             counter.increment(len(records))
    #             transfrom_and_write_records(records, resource)
    #
    #
    # def transfrom_and_write_records(self, events, stream):
    #     event_stream = stream['stream']
    #     event_schema = stream['schema']
    #     event_mdata = metadata.to_map(stream['metadata'])
    #
    #     with Transformer() as transformer:
    #         for event in events:
    #             singer.write_record(
    #                 event_stream,
    #                 transformer.transform(
    #                     event, event_schema, event_mdata
    #                 ))