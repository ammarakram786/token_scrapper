import scrapy
import pandas as pd
from datetime import datetime, timedelta
import json
import time


class DextoolsSpider(scrapy.Spider):
    name = "dextools"

    def __init__(self, input_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_file = input_file
        self.tokens = pd.read_csv(input_file).to_dict('records')
        self.milestones = [1.5, 2, 3, 5, 20, 100]

    def start_requests(self):
        for token in self.tokens:
            url = f"https://www.dextools.io/app/en/solana/pair-explorer/{token['Token Address']}"
            meta = {'token': token, 'original_address': token['Token Address']}
            yield scrapy.Request(url, callback=self.parse_token, meta=meta, dont_filter=True)

    def parse_token(self, response):
        token = response.meta['token']
        original_address = response.meta['original_address']
        final_address = self.extract_final_address(response)

        # Prepare API URL for price history
        purchase_time = datetime.strptime(token['Purchase Timestamp'], "%Y-%m-%d %H:%M")
        from_ts = int(purchase_time.timestamp())
        to_ts = int(datetime.now().timestamp())

        # Construct API URL for DEXTools
        api_url = (
            f"https://public-api.dextools.io/chain-solana/v2/pair/{final_address}/chart/candles"
            f"?resolution=5&from={from_ts}&to={to_ts}"
        )

        # Add headers to mimic browser request
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        meta = {
            'token': token,
            'original_address': original_address,
            'final_address': final_address,
            'purchase_time': purchase_time,
        }
        
        yield scrapy.Request(
            api_url, 
            callback=self.parse_price_history, 
            meta=meta, 
            headers=headers,
            dont_filter=True
        )

    def extract_final_address(self, response):
        # Check if we were redirected to a different address
        url_parts = response.url.rstrip('/').split('/')
        if 'pair-explorer' in url_parts:
            idx = url_parts.index('pair-explorer')
            if idx + 1 < len(url_parts):
                final_address = url_parts[idx + 1]
                if final_address != response.meta['original_address']:
                    self.logger.info(f"Token redirected from {response.meta['original_address']} to {final_address}")
                return final_address

        # Check for migration/redirection banner in the page
        migration_banner = response.css('div.alert-warning::text, div.alert-info::text').get()
        if migration_banner and 'migrated' in migration_banner.lower():
            # Try to extract new address from banner
            # This is a placeholder - you'll need to adjust the selector based on actual HTML
            new_address = response.css('div.alert-warning a::attr(href), div.alert-info a::attr(href)').get()
            if new_address:
                return new_address.split('/')[-1]

        return response.meta['original_address']

    def parse_price_history(self, response):
        token = response.meta['token']
        original_address = response.meta['original_address']
        final_address = response.meta['final_address']
        purchase_time = response.meta['purchase_time']

        try:
            data = json.loads(response.text)
            candles = data.get('data', [])
            
            if not candles:
                self.logger.warning(f"No price data found for {final_address}")
                return self.generate_empty_result(token, original_address, final_address)

            price_history = []
            for candle in candles:
                try:
                    ts = datetime.fromtimestamp(candle['t'])
                    price = float(candle['h'])  # Use high price for milestone checks
                    price_history.append({'timestamp': ts, 'price': price})
                except (KeyError, ValueError) as e:
                    self.logger.error(f"Error processing candle: {e}")
                    continue

            return self.process_price_history(token, original_address, final_address, purchase_time, price_history)

        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON response for {final_address}")
            return self.generate_empty_result(token, original_address, final_address)
        except Exception as e:
            self.logger.error(f"Unexpected error processing {final_address}: {e}")
            return self.generate_empty_result(token, original_address, final_address)

    def process_price_history(self, token, original_address, final_address, purchase_time, price_history):
        purchase_price = float(token['Purchase Price'])
        
        # Filter price history after purchase_time
        filtered = [p for p in price_history if p['timestamp'] >= purchase_time]
        max_price = max([p['price'] for p in filtered], default=purchase_price)

        result = {
            'ID': token['ID'],
            'Original Address': original_address,
            'Final Address': final_address,
            'Purchase Price': purchase_price,
            'Max Price': max_price,
            'DOA': 'YES' if max_price <= purchase_price else 'NO'
        }

        # Calculate milestones
        for m in self.milestones:
            hit = next((p for p in filtered if p['price'] >= purchase_price * m), None)
            result[f'Over {m}x'] = 'YES' if hit else 'NO'
            result[f'Time to {m}x'] = str(hit['timestamp'] - purchase_time) if hit else ''

        return result

    def generate_empty_result(self, token, original_address, final_address):
        return {
            'ID': token['ID'],
            'Original Address': original_address,
            'Final Address': final_address,
            'Purchase Price': token['Purchase Price'],
            'Max Price': token['Purchase Price'],
            'DOA': 'YES'
        }

    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'results.csv',
        'DOWNLOADER_MIDDLEWARES': {
            'tokenscrapper.middlewares.ProxyMiddleware': 350,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
        },
        'RETRY_TIMES': 5,
        'DOWNLOAD_TIMEOUT': 30,
        'CONCURRENT_REQUESTS': 8,
    }
