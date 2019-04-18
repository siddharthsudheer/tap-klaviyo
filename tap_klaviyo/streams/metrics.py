from tap_klaviyo.context import Context
from tap_klaviyo.streams.base import Stream

class Metrics(Stream):
    name = 'metrics'
    endpoint = "/v1/metrics"

Context.stream_objects['metrics'] = Metrics
