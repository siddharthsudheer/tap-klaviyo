from tap_klaviyo.context import Context
from tap_klaviyo.streams.base import Stream

class Campaigns(Stream):
    name = 'campaigns'
    endpoint = "/v1/campaigns"

Context.stream_objects['campaigns'] = Campaigns
