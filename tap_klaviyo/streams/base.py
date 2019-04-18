import math
import functools
import datetime
from datetime import datetime, timedelta
import sys
import simplejson
import singer
from singer import utils
from tap_klaviyo.context import Context
import asyncio
import aiohttp
from urllib.parse import urlencode
from singer import Transformer
import requests
import backoff

LOGGER = singer.get_logger()

MAX_RETRIES = 5
KLAVIYO_BASE_URL = 'https://a.klaviyo.com/api'
RESULTS_PER_PAGE = 100


def retry_pattern(backoff_type, **wait_gen_kwargs):
    def log_retry_attempt(details):
        LOGGER.info(details)
        LOGGER.info('Caught retryable error after {0} tries. Waiting {1} more seconds then retrying...'.format(
            details["tries"],
            details["wait"]))

    return backoff.on_exception(
        backoff_type,
        (requests.exceptions.RequestException,
        aiohttp.ClientError,
        aiohttp.ServerTimeoutError),
        on_backoff=log_retry_attempt,
        giveup=lambda e: e.response is not None and e.response.status_code in range(400, 499),
        **wait_gen_kwargs)


class Stream():
    # Used for bookmarking and stream identification. Is overridden by
    # subclasses to change the bookmark key.
    name = None
    replication_key = None
    endpoint = None
    schema = None
    key_properties = ['id']
    result_key = "data"

    def get_bookmark(self):
        bookmark = (singer.get_bookmark(Context.state,
                                        # name is overridden by some substreams
                                        self.name,
                                        self.replication_key)
                    or Context.config["start_date"])
        return utils.strptime_with_tz(bookmark)

    def update_bookmark(self, bookmark_value, bookmark_key=None):
        # NOTE: Bookmarking can never be updated to not get the most
        # recent thing it saw the next time you run, because the querying
        # only allows greater than or equal semantics.
        singer.write_bookmark(
            Context.state,
            # name is overridden by some substreams
            self.name,
            bookmark_key or self.replication_key,
            bookmark_value
        )
        singer.write_state(Context.state)

    # This function can be overridden by subclasses for specialized API
    # interactions.
    @retry_pattern(backoff.expo, max_tries=MAX_RETRIES, factor=10)
    def call_api(self, params):
        url = KLAVIYO_BASE_URL + self.endpoint
        LOGGER.info("GET {}?{}".format(url, urlencode({**params, 'api_key': 'pk_XXXXXXX'})))
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        result = resp.json()
        return result


    def get_objects(self):
        config_end_date = Context.config.get("end_date", False)
        end_date = utils.strptime_with_tz(config_end_date) if config_end_date else utils.now().replace(microsecond=0)
        page = 0

        while True:
            query_params = {
                'api_key': Context.config['api_key'],
                'page': page,
                'count': RESULTS_PER_PAGE
            }
            objects = self.call_api(query_params)
            
            for obj in objects[self.result_key]:
                yield obj

            # You know you're at the end when the current page has
            # less than the request size limits you set.
            if objects['end'] < objects['total'] - 1:
                page += 1
            else:
                self.update_bookmark(utils.strftime(end_date), bookmark_key="latest_pull_date")
                break

            


    def sync(self):
        """Yield's processed SDK object dicts to the caller.

        This is the default implementation. Get's all of self's objects
        and calls to_dict on them with no further processing.
        """
        for obj in self.get_objects():
            yield obj