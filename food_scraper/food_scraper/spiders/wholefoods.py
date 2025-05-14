import scrapy
import json
import time
from datetime import datetime
from scrapy import signals
from scrapy.loader import ItemLoader
from food_scraper.items import StoreItem, ProductItem


class WholeFoodsSpider(scrapy.Spider):
    name = "wholefoods"
    allowed_domains = ["www.wholefoodsmarket.com", "proxy.scrapeops.io"]
    start_urls = ["https://www.wholefoodsmarket.com"]

    store_ids = [10509]
    categories = [
        'produce',
        'dairy-eggs',
        'meat',
        # 'prepared-foods',
        # 'wine-beer-spirits',
        'pantry-essentials',
        'breads-rolls-bakery',
        'desserts',
        'supplements',
        'frozen-foods',
        'snacks-chips-salsas-dips',
        'seafood',
        'beverages'
    ]
    limit = 60

    # Track current store and category being processed
    current_store_id = None
    current_category = None

    # For request queuing system
    build_id = None
    build_id_available = False
    product_detail_queue = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WholeFoodsSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened,
                                signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        self.start_time = time.time()
        self.start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"Spider opened at {self.start_datetime}")

    def spider_closed(self, spider):
        self.end_time = time.time()
        self.end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        runtime_seconds = self.end_time - self.start_time
        runtime_minutes = runtime_seconds / 60

        # Get stats from the crawler
        stats = spider.crawler.stats.get_stats()

        # Create a dictionary with the information we want to save
        spider_stats = {
            'spider_name': self.name,
            'start_time': self.start_datetime,
            'end_time': self.end_datetime,
            'runtime_seconds': round(runtime_seconds, 2),
            'runtime_minutes': round(runtime_minutes, 2),
            'store_ids': self.store_ids,
            'categories': self.categories,
            'item_scraped_count': stats.get('item_scraped_count', 0),
            'response_received_count': stats.get('response_received_count', 0),
            'request_count': stats.get('downloader/request_count', 0),
            'status_200_count': stats.get('downloader/response_status_count/200', 0),
            'status_404_count': stats.get('downloader/response_status_count/404', 0),
            'status_500_count': stats.get('downloader/response_status_count/500', 0),
        }

        # Save stats to a JSON file
        filename = f"wholefoods_spider_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(spider_stats, f, indent=4)

        self.logger.info(f"Spider closed at {self.end_datetime}")
        self.logger.info(
            f"Total runtime: {runtime_seconds:.2f} seconds ({runtime_minutes:.2f} minutes)")
        self.logger.info(f"Stats saved to {filename}")

    def start_requests(self):
        """Start by requesting the main page to get buildId (called only once)."""
        self.logger.info(
            f"Starting spider with store_ids={self.store_ids}, categories={self.categories}")
        url = 'https://www.wholefoodsmarket.com/'
        yield scrapy.Request(
            url=url,
            callback=self.parse,
            # Enable JavaScript rendering for NEXT_DATA with more options
            meta={
                'sops_render_js': True,
                'sops_wait_for': 10,  # Wait 10 seconds for JS to load
                'sops_keep_headers': True,  # Keep browser headers
                'sops_residential': True,  # Use residential IP
                'sops_country': 'us',  # Use US IP address
            },
            priority=100  # Highest priority as this is needed first
        )

    def parse(self, response):
        """Extract buildId from __NEXT_DATA__ script tag using CSS selector."""
        self.logger.info(f"Response status: {response.status}")
        next_data_text = response.css('script#__NEXT_DATA__::text').get()

        # Look for __NEXT_DATA__ with alternate selectors
        if not next_data_text:
            scripts = response.css('script').getall()
            # Try alternate selectors
            self.logger.info("Trying alternate selectors for __NEXT_DATA__")
            next_data_text = response.xpath(
                '//script[@id="__NEXT_DATA__"]/text()').get()

            # If still not found, try looking for any script containing buildId
            if not next_data_text:
                self.logger.warning(
                    "Could not find __NEXT_DATA__ with standard selectors")
                for script in scripts:
                    if "buildId" in script:
                        self.logger.info("Found script with buildId")
                        # Extract JSON from the script
                        try:
                            import re
                            json_text = re.search(r'({.*})', script).group(1)
                            next_data_text = json_text
                            break
                        except:
                            self.logger.error(
                                "Failed to extract JSON from script with buildId")

        if next_data_text:
            try:
                next_data = json.loads(next_data_text)
                self.build_id = next_data.get('buildId')
                self.build_id_available = True
                self.logger.info(f"Extracted buildId: {self.build_id}")

                # Process any queued requests now that buildId is available
                self.process_product_detail_queue()

                # Begin processing for the first store_id and category
                # Process all combinations of store_ids and categories
                for store_id in self.store_ids:
                    self.current_store_id = store_id
                    store_summary_url = f'https://www.wholefoodsmarket.com/stores/{store_id}/summary'
                    self.logger.info(
                        f"Requesting store summary from: {store_summary_url}")
                    yield scrapy.Request(
                        url=store_summary_url,
                        callback=self.parse_store_summary,
                        meta={
                            'dont_filter': True,  # Skip URL filtering
                            'sops_skip_headers': True,  # Skip headers modification by middleware
                            'sops_country': 'us',  # Use US IP address
                        },
                        headers={
                            'Accept': 'application/json',
                            'X-Requested-With': 'XMLHttpRequest',
                            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                            'Referer': 'https://www.wholefoodsmarket.com/stores/store-locator'
                        },
                        errback=self.handle_error
                    )
            except json.JSONDecodeError as e:
                self.logger.error(
                    f"Failed to parse __NEXT_DATA__ as JSON: {str(e)}")
        else:
            self.logger.error('__NEXT_DATA__ script tag not found')
            # Write response to file for debugging
            with open('wholefoods_response.html', 'wb') as f:
                f.write(response.body)
            self.logger.info(
                "Saved response to wholefoods_response.html for debugging")

    def handle_error(self, failure):
        """Handle request errors"""
        self.logger.error(f"Request failed: {failure.value}")
        self.logger.error(f"URL that failed: {failure.request.url}")

    def parse_store_summary(self, response):
        """Parse store summary JSON and yield StoreItem, then request product listings."""
        self.logger.info(f"Received store summary response: {response.status}")
        store_id = self.current_store_id

        try:
            data = response.json()
            self.logger.info(
                f"Successfully parsed store summary JSON with keys: {list(data.keys())}")

            store_loader = ItemLoader(item=StoreItem(), response=response)
            store_loader.add_value('store_id', store_id)
            store_loader.add_value('status', data.get('status'))
            store_loader.add_value('date_opened', data.get('openedAt'))

            location_data = data.get('primaryLocation', {})
            if not location_data:
                self.logger.warning(
                    "No location data available in store summary")

            store_loader.add_value('latitude', location_data.get('latitude'))
            store_loader.add_value('longitude', location_data.get('longitude'))

            address_data = location_data.get('address', {})
            if not address_data:
                self.logger.warning(
                    "No address data available in store summary")

            store_loader.add_value(
                'street', address_data.get('STREET_ADDRESS_LINE1'))
            store_loader.add_value('city', address_data.get('CITY'))
            store_loader.add_value('state', address_data.get('STATE'))
            store_loader.add_value('zip_code', address_data.get('ZIP_CODE'))
            store_loader.add_value(
                'postal_code', address_data.get('POSTAL_CODE'))

            store_item = store_loader.load_item()
            self.logger.info(f"Created store item: {store_item}")
            yield store_item

            # Request product listings for each category for this store
            for category in self.categories:
                self.current_category = category
                leaf_category_url = f"https://www.wholefoodsmarket.com/api/products/category/{category}?leafCategory={category}&store={store_id}&limit={self.limit}&offset=0"
                self.logger.info(
                    f"Requesting product listings from: {leaf_category_url}")
                yield scrapy.Request(
                    url=leaf_category_url,
                    callback=self.parse_product_listings,
                    meta={'offset': 0, 'store_id': store_id,
                          'category': category,
                          'sops_country': 'us'},
                    priority=50,  # High priority for category listings
                    errback=self.handle_error
                )
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse store summary JSON: {str(e)}")
            self.logger.error(
                f"Response text (first 200 chars): {response.text[:200]}")
            with open('store_summary_error.html', 'wb') as f:
                f.write(response.body)
            self.logger.info(
                "Saved failed response to store_summary_error.html")

    def parse_product_listings(self, response):
        """Parse product listings JSON, request details for each product, and handle pagination."""
        self.logger.info(
            f"Received product listings response: {response.status}")

        try:
            data = response.json()
            products = data.get('results', [])
            offset = response.meta['offset']
            store_id = response.meta['store_id']
            category = response.meta['category']

            self.logger.info(
                f"Found {len(products)} products at offset {offset} for store {store_id}, category {category}")

            # When offset=0, yield all pagination requests based on total
            if offset == 0:
                category_refinements = data.get('facets', [{}])[
                    0].get('refinements', [])
                total_products = [cr for cr in category_refinements if cr.get(
                    'slug') == category]

                if total_products:
                    total_count = total_products[0].get('count', 0)
                    self.logger.info(
                        f"Total products in category '{category}' for store {store_id}: {total_count}")
                    num_pages = (total_count + self.limit - 1) // self.limit

                    for page in range(1, num_pages):
                        next_offset = page * self.limit
                        next_url = f"https://www.wholefoodsmarket.com/api/products/category/{category}?leafCategory={category}&store={store_id}&limit={self.limit}&offset={next_offset}"
                        yield scrapy.Request(
                            url=next_url,
                            callback=self.parse_product_listings,
                            meta={'offset': next_offset,
                                  'store_id': store_id,
                                  'category': category,
                                  'sops_country': 'us'},
                            priority=40,  # Medium priority for pagination
                            errback=self.handle_error
                        )
                else:
                    self.logger.warning(
                        f"Could not find category refinement for '{category}'")

            # Process current products
            for i, product in enumerate(products):
                product_loader = ItemLoader(
                    item=ProductItem(), response=response)
                product_loader.add_value('name', product.get('name'))
                product_loader.add_value('price', product.get('regularPrice'))
                product_loader.add_value('slug', product.get('slug'))
                product_loader.add_value('brand', product.get('brand'))
                product_loader.add_value('store_id', store_id)
                # Add the current category from the URL - will be overridden if more
                # specific category info is found in product details
                product_loader.add_value('category', category)

                if not self.build_id_available:
                    # Queue this request for later when buildId is available
                    self.logger.info(
                        f"Queuing product detail request for {product.get('name')}, buildId not yet available")
                    self.product_detail_queue.append({
                        'url_slug': product.get('slug'),
                        'store_id': store_id,
                        'product_loader': product_loader,
                        'category': category  # Store category in queue
                    })
                else:
                    # buildId is available, make the request now
                    yield self.make_product_detail_request(product.get('slug'), store_id, product_loader, category)
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Failed to parse product listings JSON: {str(e)}")
            with open('product_listings_error.html', 'wb') as f:
                f.write(response.body)
            self.logger.info(
                "Saved failed response to product_listings_error.html")

    def make_product_detail_request(self, slug, store_id, product_loader, category=None):
        """Create a product detail request with the given parameters"""
        product_detail_url = (
            f'https://www.wholefoodsmarket.com/_next/data/{self.build_id}'
            f'/product/{slug}.json?store={store_id}'
        )
        return scrapy.Request(
            url=product_detail_url,
            callback=self.parse_product_details,
            meta={'product_loader': product_loader,
                  'sops_country': 'us', 'category': category},
            priority=30,  # Lower priority for individual product details
            errback=self.handle_error
        )

    def process_product_detail_queue(self):
        """Process any queued product detail requests now that buildId is available"""
        if not self.build_id_available:
            self.logger.warning(
                "Attempted to process queue but buildId not available")
            return

        queue_count = len(self.product_detail_queue)
        self.logger.info(
            f"Processing {queue_count} queued product detail requests")

        for item in self.product_detail_queue:
            request = self.make_product_detail_request(
                item['url_slug'],
                item['store_id'],
                item['product_loader'],
                item.get('category')  # Pass category from queue
            )
            self.crawler.engine.crawl(request, self)

        # Clear the queue after processing
        self.product_detail_queue = []
        self.logger.info(
            f"Queue processed, {queue_count} requests added to crawler")

    def parse_product_details(self, response):
        """Parse product details JSON and combine with listing data using ItemLoader."""
        try:
            product_loader = response.meta['product_loader']
            product_data = response.json()
            # Get category from meta if available
            url_category = response.meta.get('category')

            if 'pageProps' not in product_data:
                self.logger.warning(f"Missing 'pageProps' in product details")
                return

            product_detail_data = product_data.get(
                'pageProps', {}).get('data', {})

            # Get nutrition elements before anything else
            nutrition_elements = product_detail_data.get('nutritionElements')

            # Skip this product if it has no nutrition elements or if they'll all be filtered out
            if not nutrition_elements:
                self.logger.info(
                    f"Skipping product with no nutrition elements: {product_detail_data.get('name')}")
                return

            # Check if any nutrition elements have valid amount_per_serving values
            has_valid_nutrition = False
            for elem in nutrition_elements:
                amount = elem.get('perServing')
                if amount is not None and amount != 0 and amount != '' and amount != '0':
                    has_valid_nutrition = True
                    break

            if not has_valid_nutrition:
                self.logger.info(
                    f"Skipping product with no valid nutrition elements: {product_detail_data.get('name')}")
                return

            product_loader.add_value('asin', product_detail_data.get('asin'))
            product_loader.add_value(
                'amazon_product_id', product_detail_data.get('id'))
            product_loader.add_value('rank', product_detail_data.get('rank'))
            product_loader.add_value(
                'is_available', product_detail_data.get('isAvailable'))

            categories = product_detail_data.get('categories', {})

            # Only set category from product details if it exists, otherwise keep the one from URL
            product_category = categories.get('name')
            if product_category:
                product_loader.add_value('category', product_category)
            elif url_category:
                # Use URL category if no category in product details
                product_loader.add_value('category', url_category)

            # Extract child categories as before
            product_loader.add_value('category_2', categories.get(
                'childCategory', {}).get('name'))

            # Try to extract category_3 if it exists (from child of childCategory)
            child_category = categories.get('childCategory', {})
            if child_category and isinstance(child_category, dict) and 'childCategory' in child_category:
                product_loader.add_value('category_3', child_category.get(
                    'childCategory', {}).get('name'))

            product_loader.add_value('diets', product_detail_data.get('diets'))
            product_loader.add_value(
                'ingredients', product_detail_data.get('ingredients'))
            product_loader.add_value(
                'allergens', product_detail_data.get('allergens'))
            product_loader.add_value(
                'additives', product_detail_data.get('additives'))
            product_loader.add_value(
                'certifications', product_detail_data.get('certifications'))
            product_loader.add_value(
                'nutrition_group', product_detail_data.get('nutritionGroup'))
            product_loader.add_value(
                'nutrition_label_format', product_detail_data.get('nutritionLabelFormat'))
            product_loader.add_value(
                'nutrition_elements', nutrition_elements)
            product_loader.add_value(
                'serving_info', product_detail_data.get('servingInfo'))
            product_loader.add_value(
                'is_alcoholic', product_detail_data.get('isAlcoholic'))
            product_loader.add_value(
                'unit_of_measure', product_detail_data.get('uom'))

            images = product_detail_data.get('images', [{}])
            if images:
                product_loader.add_value('image', images[0].get('image'))

            product_loader.add_value(
                'related_products', product_detail_data.get('related'))

            yield product_loader.load_item()
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Failed to parse product details JSON: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error processing product details: {str(e)}")
