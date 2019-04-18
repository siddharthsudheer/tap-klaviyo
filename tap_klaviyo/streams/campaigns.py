import singer
from singer import utils
import math
import backoff
import asyncio
import aiohttp
from urllib.parse import urlencode
from tap_klaviyo.context import Context
from tap_klaviyo.streams.base import Stream, KLAVIYO_BASE_URL, MAX_RETRIES, RESULTS_PER_PAGE, retry_pattern


LOGGER = singer.get_logger()


class Campaigns(Stream):
    name = 'campaigns'
    endpoint = "/v1/campaigns"
    
    def get_objects(self):
        config_end_date = Context.config.get("end_date", False)
        end_date = utils.strptime_with_tz(config_end_date) if config_end_date else utils.now().replace(microsecond=0)
        api_key = Context.config['api_key']

        # Getting only page 1
        query_params = {
            'api_key': api_key,
            'page': 0,
            'count': RESULTS_PER_PAGE
        }
        objects = self.call_api(query_params)

        for obj in objects[self.result_key]:
            yield obj
        
        # If total count is greater than 100, 
        # that means there are more page. We calculate 
        # the number of pages, then async get all other pages
        # except the first one we already got above.
        if objects['total'] > RESULTS_PER_PAGE:
            num_pages = math.ceil((objects['total'] - RESULTS_PER_PAGE)/RESULTS_PER_PAGE)
            jobs = [
                {   
                    'result_key': self.result_key,
                    'endpoint': self.endpoint,
                    'query_params': {
                        'api_key': api_key,
                        'page': i,
                        'count': RESULTS_PER_PAGE
                    }
                } for i in range(1, num_pages+1)
            ]
            other_pages_objects = GetAsync(jobs).Run()
            
            for obj in other_pages_objects:
                yield obj

        self.update_bookmark(utils.strftime(end_date), bookmark_key="latest_pull_date")
        
Context.stream_objects['campaigns'] = Campaigns


########################################################################################
# HELPERS FOR CAMPAIGN INSIGHTS
########################################################################################

class GetAsync():
    def __init__(self, jobs):
        self.jobs = jobs
        self.results = []

    # @retry_pattern(backoff.expo, max_tries=MAX_RETRIES, factor=10)
    async def _get_async(self, url, headers=None, params=None, retry_attempt=0):
        headers = {**headers, "Connection": "close"} if headers else {"Connection": "close"}
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(url=url, headers=headers, params=params, raise_for_status=True) as response:
                LOGGER.info("GET {}?{}".format(url, urlencode({**params, 'api_key': 'pk_{}'.format(Context.config.get('account_id', "XXXX"))})))
                if response.status == 200:
                    return await response.json()

    @retry_pattern(backoff.expo, max_tries=MAX_RETRIES, factor=10)
    async def _request(self, job):
        url = KLAVIYO_BASE_URL + job['endpoint']
        resp = await self._get_async(url, params=job['query_params'])
        return resp[job['result_key']]

    async def _runner(self):
        futures = [self._request(i) for i in self.jobs]
        for i, future in enumerate(asyncio.as_completed(futures)):
            results = await future
            self.results += results
    
    def Run(self):
        asyncio.run(self._runner())
        return self.results