import singer
from singer import utils
from tap_klaviyo.context import Context
from tap_klaviyo.streams.base import Stream, KLAVIYO_BASE_URL, MAX_RETRIES, retry_pattern
from datetime import datetime, timedelta
import asyncio
import aiohttp
from urllib.parse import urlencode
import backoff


LOGGER = singer.get_logger()
DATE_RANGE_CHUNK_SIZE = 10
DATE_FMT = "%Y-%m-%d"

class CampaignInsights(Stream):
    name = 'campaign_insights'
    endpoint = "/v1/metric/{metric_id}/export"
    replication_key = "insight_date"
    key_properties = ['campaign_id', 'metric_id', 'insight_date']

    campaign_insights_metrics = {
        "Placed Order": {"insights":{"Revenue":"value", "Total Conversion":"count", "Unique Conversion":"unique"}, "where":"$attributed_message"},
        "Clicked Email": {"insights":{"Total Clicked":"count", "Unique Clicked":"unique"}, "where":"$message"},
        "Opened Email": {"insights":{"Total Opened":"count", "Unique Opened":"unique"}, "where":"$message"},
        "Unsubscribed": {"insights":{"Total Unsubscribed":"count", "Unique Unsubscribed":"unique"}, "where":"$message"},
        "Marked Email as Spam": {"insights":{"Total Spammed":"count", "Unique Spammed":"unique"}, "where":"$message"},
        "Received Email": {"insights":{"Total Received":"count", "Unique Received":"unique"}, "where":"$message"},
        "Bounced Email": {"insights":{"Total Bounced":"count", "Unique Bounced":"unique"}, "where":"$message"},
        "Dropped Email": {"insights":{"Total Dropped":"count", "Unique Dropped":"unique"}, "where":"$message"}
    }

    def get_metrics(self):
        stream = Context.stream_objects['metrics']()
        all_metrics = stream.sync()
        final = {}
        for m in all_metrics:
            if m['name'] in self.campaign_insights_metrics.keys():
                final[m['name']] = {'metric_id': m['id'], 'metric_name': m['name'], **self.campaign_insights_metrics[m['name']]}
        return final

    def get_campaigns(self):
        stream = Context.stream_objects['campaigns']()
        campaigns = stream.sync()
        return campaigns

    
    def get_job_configs(self, start_date, end_date, campaigns, metrics):
        date_chunks = get_chunks(start_date, end_date, DATE_RANGE_CHUNK_SIZE)

        jobs = []
        for dt in reversed(date_chunks):
            for c in campaigns:
                if c['status'] == 'sent':
                    for v in metrics.values():
                        for insight_name, measurement in v['insights'].items():
                            jobs.append({
                                'campaign_id': c['id'],
                                'metric_id': v['metric_id'],
                                'metric_name': v['metric_name'],
                                'insight_name': insight_name,
                                'endpoint': "/v1/metric/{mid}/export".format(mid=v['metric_id']),
                                'query_params': {
                                    'api_key': Context.config['api_key'],
                                    'start_date': dt['start_date'],
                                    'end_date': dt['end_date'],
                                    'measurement': measurement,
                                    'unit': 'day',
                                    'where': '[["{w}","=","{cid}"]]'.format(w=v['where'], cid=c['id'])
                                }
                            })
        return jobs

    def get_objects(self):
        metrics = self.get_metrics()
        campaigns = self.get_campaigns()
        start_date = self.get_bookmark()
        config_end_date = Context.config.get("end_date", False)
        end_date = utils.strptime_with_tz(config_end_date) if config_end_date else utils.now().replace(microsecond=0)
        
        jobs = self.get_job_configs(start_date, end_date, campaigns, metrics)
        objects = GetAsync(jobs).Run()
        
        for obj in objects:
            yield obj
        
        self.update_bookmark(utils.strftime(end_date))


Context.stream_objects['campaign_insights'] = CampaignInsights


########################################################################################
# HELPERS FOR CAMPAIGN INSIGHTS
########################################################################################

def get_chunks(st, ed, num_days=1):
    ranges = []
    
    while st < ed:
        curr_ed = st + timedelta(days=num_days)
        if curr_ed > ed:
            curr_ed = ed
        ranges.append({'start_date': st.strftime(DATE_FMT), 'end_date': curr_ed.strftime(DATE_FMT)})
        st = curr_ed + timedelta(days=1)

    return ranges


class GetAsync():
    def __init__(self, jobs):
        self.jobs = jobs
        self.results = []

    # @retry_pattern(backoff.expo, max_tries=MAX_RETRIES, factor=10)
    async def _get_async(self, url, headers=None, params=None, retry_attempt=0):
        headers = {**headers, "Connection": "close"} if headers else {"Connection": "close"}
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(url=url, headers=headers, params=params, raise_for_status=True) as response:
                LOGGER.info("GET {}?{}".format(url, urlencode({**params, 'api_key': 'pk_XXXXXXX'})))
                if response.status == 200:
                    return await response.json()

    @retry_pattern(backoff.expo, max_tries=MAX_RETRIES, factor=10)
    async def _request(self, job):
        url = KLAVIYO_BASE_URL + job['endpoint']
        results = []
        resp = await self._get_async(url, params=job['query_params'])
        for met in resp['results']:
            for d in met['data']:
                result = {
                    "campaign_id": job['campaign_id'],
                    "metric_id": job['metric_id'],
                    "metric_name": job['metric_name'],
                    "insight_date": d["date"],
                    "insight_name": job['insight_name'],
                    "insight_value": d['values'][0]
                }
                results.append(result)
        return results

    async def _runner(self):
        futures = [self._request(i) for i in self.jobs]
        for i, future in enumerate(asyncio.as_completed(futures)):
            results = await future
            self.results += results
    
    def Run(self):
        asyncio.run(self._runner())
        return self.results