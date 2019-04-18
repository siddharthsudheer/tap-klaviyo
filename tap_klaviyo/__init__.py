#!/usr/bin/env python3
import os
import datetime
import json
import time
import math
import singer
from singer import utils
from singer import metadata
from singer import Transformer
from tap_klaviyo.context import Context
import tap_klaviyo.streams # Load stream objects into Context

REQUIRED_CONFIG_KEYS = ["start_date", "api_key"]

LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    # This schema represents many of the currency values as JSON schema
    # 'number's, which may result in lost precision.
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        schema_name = filename.replace('.json', '')
        with open(path) as file:
            schemas[schema_name] = json.load(file)

    return schemas


def get_discovery_metadata(stream, schema):
    autoselect = Context.config.get('autoselect_all_for_discover', False)
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'selected', autoselect)
    mdata = metadata.write(mdata, (), 'table-key-properties', stream.key_properties)
    if stream.replication_key:
        mdata = metadata.write(mdata, (), 'forced-replication-method', 'INCREMENTAL')
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [stream.replication_key])
    else:
        mdata = metadata.write(mdata, (), 'forced-replication-method', 'FULL_TABLE')

    for field_name in schema['properties'].keys():
        if field_name in stream.key_properties or field_name == stream.replication_key:
            mdata = metadata.write(mdata, ('properties', field_name), 'selected', True)
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'selected', autoselect)
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        if schema_name not in Context.stream_objects:
            continue

        stream = Context.stream_objects[schema_name]()

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : get_discovery_metadata(stream, schema),
            'key_properties': stream.key_properties
        }
        if stream.replication_key:
            catalog_entry['replication_key'] = stream.replication_key
            catalog_entry['replication_method'] = 'INCREMENTAL'
        else:
            catalog_entry['replication_method'] = 'FULL_TABLE'

        streams.append(catalog_entry)

    return {'streams': streams}


def sync():
    # Emit all schemas first so we have them for child streams
    for stream in Context.catalog["streams"]:
        if Context.is_selected(stream["tap_stream_id"]):
            singer.write_schema(stream["tap_stream_id"],
                                stream["schema"],
                                stream["key_properties"],
                                bookmark_properties=stream.get("replication_key", None))
            Context.counts[stream["tap_stream_id"]] = 0
            Context.durations[stream["tap_stream_id"]] = None


    # Loop over streams in catalog
    for catalog_entry in Context.catalog['streams']:
        stream_start_time = time.time()
        stream_id = catalog_entry['tap_stream_id']
        stream = Context.stream_objects[stream_id]()
        stream.schema = catalog_entry['schema']

        if not Context.is_selected(stream_id):
            LOGGER.info('Skipping stream: %s', stream_id)
            continue

        LOGGER.info('Syncing stream: %s', stream_id)

        if not Context.state.get('bookmarks'):
            Context.state['bookmarks'] = {}
        Context.state['bookmarks']['currently_sync_stream'] = stream_id

        with Transformer() as transformer:
            for rec in stream.sync():
                extraction_time = singer.utils.now()
                record_metadata = metadata.to_map(catalog_entry['metadata'])
                rec = transformer.transform(rec, stream.schema, record_metadata)
                singer.write_record(stream_id,
                                    rec,
                                    time_extracted=extraction_time)
                Context.counts[stream_id] += 1

        Context.state['bookmarks'].pop('currently_sync_stream')
        singer.write_state(Context.state)
        stream_job_duration = time.strftime("%H:%M:%S", time.gmtime(time.time() - stream_start_time))
        Context.durations[stream_id] = stream_job_duration

    div = "-"*50
    info_msg = "\n{d}".format(d=div)
    info_msg += "\nAccount: {}".format(Context.config['account_id'])
    info_msg += "\n{d}".format(d=div)
    for stream_id, stream_count in Context.counts.items():
        info_msg += "\n{}: {}".format(stream_id, stream_count)
        info_msg += "\nDuration: {}".format(Context.durations[stream_id])
    info_msg += "\n{d}\n".format(d=div)
    LOGGER.info(info_msg)

@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    Context.config = args.config

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        Context.tap_start = utils.now()
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()

        Context.state = args.state
        sync()

if __name__ == "__main__":
    main()
