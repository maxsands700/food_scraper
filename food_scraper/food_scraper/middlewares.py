# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from urllib.parse import urlencode
from scrapy import Request
import json
import random
import requests

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class FoodScraperSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn't have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class FoodScraperDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ScrapeOpsProxyMiddleware:

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.scrapeops_api_key = settings.get('SCRAPEOPS_API_KEY')
        self.scrapeops_endpoint = 'https://proxy.scrapeops.io/v1/?'
        self.scrapeops_proxy_active = settings.get(
            'SCRAPEOPS_PROXY_ENABLED', False)

    @staticmethod
    def _param_is_true(request, key):
        if request.meta.get(key) or request.meta.get(key, 'false').lower() == 'true':
            return True
        return False

    @staticmethod
    def _replace_response_url(response):
        real_url = response.headers.get(
            'Sops-Final-Url', def_val=response.url)
        return response.replace(
            url=real_url.decode(response.headers.encoding))

    def _get_scrapeops_url(self, request):
        payload = {'api_key': self.scrapeops_api_key, 'url': request.url}
        if self._param_is_true(request, 'sops_render_js'):
            payload['render_js'] = True
        if self._param_is_true(request, 'sops_residential'):
            payload['residential'] = True
        if self._param_is_true(request, 'sops_keep_headers'):
            payload['keep_headers'] = True
        if request.meta.get('sops_country') is not None:
            payload['country'] = request.meta.get('sops_country')
        proxy_url = self.scrapeops_endpoint + urlencode(payload)
        return proxy_url

    def _scrapeops_proxy_enabled(self):
        if self.scrapeops_api_key is None or self.scrapeops_api_key == '' or self.scrapeops_proxy_active == False:
            return False
        return True

    def process_request(self, request, spider):
        if self._scrapeops_proxy_enabled is False or self.scrapeops_endpoint in request.url:
            return None

        scrapeops_url = self._get_scrapeops_url(request)
        new_request = request.replace(
            cls=Request, url=scrapeops_url, meta=request.meta)
        return new_request

    def process_response(self, request, response, spider):
        new_response = self._replace_response_url(response)
        return new_response


class ScrapeOpsFakeBrowserHeadersMiddleware:
    """Middleware to fetch and use fake browser headers from ScrapeOps API"""

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.scrapeops_api_key = settings.get('SCRAPEOPS_API_KEY')
        self.scrapeops_endpoint = 'http://headers.scrapeops.io/v1/browser-headers'
        self.scrapeops_fake_headers_active = settings.get(
            'SCRAPEOPS_FAKE_HEADERS_ENABLED', True)
        self.scrapeops_num_results = settings.get('SCRAPEOPS_NUM_RESULTS', 5)
        self.headers_list = []
        self._get_headers_list()

    def _get_headers_list(self):
        """Get fake browser headers from ScrapeOps API"""
        payload = {'api_key': self.scrapeops_api_key}
        if self.scrapeops_num_results > 0:
            payload['num_results'] = self.scrapeops_num_results

        try:
            response = requests.get(self.scrapeops_endpoint, params=payload)
            json_response = response.json()
            self.headers_list = json_response.get('result', [])
        except Exception as e:
            print(f'Error fetching fake browser headers: {e}')

    def _get_random_header(self):
        """Get a random header from the list"""
        if not self.headers_list:
            return {}

        # Get a random header from the list
        random_index = random.randint(0, len(self.headers_list) - 1)
        return self.headers_list[random_index]

    def _fake_headers_enabled(self):
        """Check if fake headers are enabled"""
        if self.scrapeops_api_key is None or self.scrapeops_api_key == '' or not self.scrapeops_fake_headers_active:
            return False
        return True

    def process_request(self, request, spider):
        """Process request to add fake browser headers"""
        # Skip if fake headers are not enabled or no headers available
        if not self._fake_headers_enabled() or not self.headers_list:
            return None

        # Don't modify requests for the proxy itself or for requests that already have headers set
        if self.scrapeops_endpoint in request.url or 'sops_skip_headers' in request.meta:
            return None

        # Add fake browser headers to the request
        random_header = self._get_random_header()
        for key, value in random_header.items():
            request.headers[key] = value

        return None
