############                dextools

import csv
import json
import scrapy
from datetime import datetime


class dextools(scrapy.Spider):
    name = 'dextools'
    url = "https://www.dextools.io/shared/search/pair?query={}&strict=true"
    # url = "https://www.dextools.io/shared/search/pair?query=83mcrqjzvkmeqd9wjbzductpgrbzmdopdmsx5sf1pump&strict=true"
    old_url = 'https://www.dextools.io/app/en/solana/pair-explorer/{}'
    # old_url = 'https://www.dextools.io/app/en/solana/pair-explorer/AFMyfmGLmZ7VGfqbQ3aUaHtNQnH3mE4T1foJyimppump'
    redirect_url = 'https://www.dextools.io/shared/data/pair?address={}&chain=solana&audit=true&locks=true'
    # redirect_url = 'https://www.dextools.io/shared/data/pair?address=AFMyfmGLmZ7VGfqbQ3aUaHtNQnH3mE4T1foJyimppump&chain=solana&audit=true&locks=true'
    headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
            'content-type': 'application/json',
            'priority': 'u=1, i',
            'referer': 'https://www.dextools.io/app/en/solana/pair-explorer/EGZ7tiLeH62TPV1gL8WwbXGzEPa9zmcpVnnkPKKnrE2U?t=1746557811528',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            # 'cookie': '_pk_id.1.b299=88a9da58ae9a6f5d.1746518556.; AWSALB=DwG/axn290gGJ2tXPpbT/uJ0ZL+wnlN2Ll6cP1JCWxjNvFKX3VkAYHas55x77dUdh0X2jJe6M6SktKWzU2Ut0b0g8iiNJNfnDpiMkQ88QkiqPtX0kWUuKhHRfViK; AWSALBCORS=DwG/axn290gGJ2tXPpbT/uJ0ZL+wnlN2Ll6cP1JCWxjNvFKX3VkAYHas55x77dUdh0X2jJe6M6SktKWzU2Ut0b0g8iiNJNfnDpiMkQ88QkiqPtX0kWUuKhHRfViK; __cf_bm=n.RXLzwzYo3v4eR5bYwE3xtYMMKEMeTkOYCu5RNwyak-1746557804-1.0.1.1-3NJC3.4ToKssSk9mQYxtWMIS36ges_o.wJeprUVNQQYdZj3MRa5unWk19ATfAQrPN0xlqSOnLZ93z4QSbBMUV9o8O_b17izuIrVA_nBj3es; _pk_ses.1.b299=1; cf_clearance=ngBvdUSkwvIyY4mqbrGz2yWrg8M7k53B3muXmTZsn5I-1746558611-1.2.1.1-iKmvwEm34P5_cHHbxpNICQSOEPh6FAUWuV3qjHBK3vqKKUyLCKdQ1PjM9uJk74._a8Q00wHItML5Yf_ReYEdxHyn1vbpr8Q1vUbRUpMkg_NSVpln58d6j6kz5rRFmH0e93gn2AAoj3gyglKHyRgzJUnsAD7mMy5TCY3Nz0Uz7hJDT.K6QFb8utGWudE0Azb1RcPDCZgBixKuG7.pQb5KRU9LXpN5ByBIevB5N1dq_tQx.P3uTfTx7vZYh7hqjjrOWypRGVMXz3BZr5oKTR4fkzfeDYVHSV2Wceaqb.Q6Miseo7WWwW32zN1E0qfqf5OXNHbU.6WVkM6ywMjo7SgrAwVXJO1rERve7miM4PH11Czlf2iBYIabakx8sb4XYKKt',
        }

    custom_settings = {
        'FEED_URI': f'output/dextools {datetime.now().strftime("%d-%m-%Y %H-%M")}.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
    }

    def start_requests(self):
        rows = []
        with open("input/dextools_input.csv", 'r', newline='', encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                rows.append(row) if row else rows.append({})

        for index, row in enumerate(rows, start=1):     ##   TODO:  Added Slicing for testing only
            if row.get('ID'):
                pass
            else:
                row['ID'] = f'row_{index}'
            search_token = row.get('Token Address','')
            redirect_url = self.redirect_url.format(search_token)
            yield scrapy.Request(redirect_url, callback=self.redirect_parse, headers=self.headers, meta={'row': row})


    def redirect_parse(self, response):
        data = json.loads(response.text)
        redirected_id = response.meta.get('row',{}).get('Token Address', '')
        for row in data.get('data'):
            redirected_id = row.get('redirectToPool','')
            break   ##  This break is important - Keep it

        row = response.meta.get('row')
        search_token = row.get('Token Address', '')
        search_url = self.url.format(search_token)
        print('Search URL is :', search_url, ' || row is :', row)
        offset = 0
        yield scrapy.Request(search_url, callback=self.parse, headers=self.headers,
                             meta={'row': row, 'offset': offset, 'redirected_id': redirected_id})

    def parse(self, response):
        row = response.meta.get('row')
        offset = response.meta.get('offset')
        redirected_id = response.meta.get('redirected_id')
        purchase_price = float(row.get('Purchase Price'))
        purchase_time = row['Purchase Timestamp'].strip().replace('\n','').replace('\t','').replace('\r','').replace('\xa0','')
        purchase_timestamp = datetime.strptime(purchase_time, '%Y-%m-%d %H:%M:%S')

        multipliers = [1.5, 2, 3, 5, 20, 100]
        thresholds = {x: purchase_price * x for x in multipliers}
        time_reached = {x: None for x in multipliers}

        ever_above = False
        if json.loads(response.text):
            data = json.loads(response.text)
            max_price = float(row.get('Purchase Price'))
            for record in data.get('results',[]):
                offset += 1
                # print(offset, "# price = ", record.get('price',''), ' || for row = ', row)

                price = record.get('price','')
                price_time = datetime.strptime(record.get('priceTime','').replace('Z', ''), '%Y-%m-%dT%H:%M:%S.%f')
                if price > purchase_price:
                    ever_above = True
                if price > max_price:
                    max_price = price
                for m in multipliers:
                    if price >= thresholds[m] and time_reached[m] is None:
                        time_reached[m] = price_time - purchase_timestamp

            item = dict()
            item['ID'] = row.get('ID','')
            item['Original Address'] = self.old_url.format(row.get('Token Address',''))
            item['Final Address'] = self.old_url.format(redirected_id)
            item['Purchase Price'] = row.get('Purchase Price')
            item['Max Price'] = max_price
            item['DOA'] = 'Yes' if ever_above else 'No'

            for m in multipliers:
                item[f'Over {m}x'] = 'YES' if time_reached[m] else 'NO'
                item[f'Time to {m}x'] = str(time_reached[m]) if time_reached[m] else ''

            yield item
