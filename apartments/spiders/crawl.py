# -*- coding: utf-8 -*-
import scrapy
import json
from datetime import datetime
from apartments.items import ApartmentsItem


class CrawlSpider(scrapy.Spider):
    name = 'apartments'
    allowed_domains = ['otodom.pl']

    headers = {
        'authority': 'www.otodom.pl',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Chromium";v="86", "\\"Not\\\\A;Brand";v="99", "Google Chrome";v="86"',
        'sec-ch-ua-mobile': '?0',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'pl,en-US;q=0.9,en;q=0.8',
    }

    def __init__(self, category, limit=1000000, *args, **kwargs):
        self.limit = int(limit)
        categories = {
            'a': 'sprzedaz/mieszkanie',
            'b': 'wynajem/mieszkanie',
            'c': 'sprzedaz/dom',
            'd': 'wynajem/dom',
        }
        self.category = categories[category]
        super().__init__(*args, **kwargs)

    @staticmethod
    def generate_area_intervals():
        """ Split the search into more manageable segments,
            since there's no way to access more than 12000 records at once """
        area_intervals = list()
        # area_intervals.append('search%5Bfilter_float_m%3Ato%5D=30')  # Everything below 30m2
        for i in range(54, 90, 4):  # Higher granularity from 30 to 90 m2
            area_intervals.append(f'search%5Bfilter_float_m%3Afrom%5D={i}&search%5Bfilter_float_m%3Ato%5D={i+4}')
        # for i in range(90, 300, 10):  # Low granularity from 90 to 300 m2
        #     area_intervals.append(f'search%5Bfilter_float_m%3Afrom%5D={i}&search%5Bfilter_float_m%3Ato%5D={i+10}')
        # area_intervals.append('search%5Bfilter_float_m%3Afrom%5D=300')
        return area_intervals

    def start_requests(self):
        for area_interval in self.generate_area_intervals():
            start_url = f'https://www.otodom.pl/{self.category}/?{area_interval}&nrAdsPerPage=72'
            yield scrapy.Request(start_url, headers=self.headers, dont_filter=True)

    def parse(self, response):
        # Pagination
        next_page_url = response.xpath('//li[@class="pager-next"]/a/@href').get()
        if next_page_url and self.limit > 0:
            yield scrapy.Request(next_page_url, callback=self.parse, headers=self.headers)
        # Actual listings
        ad_urls = response.xpath('//div[@class="offer-item-details"]/header/h3/a/@href').getall()
        for ad_url in ad_urls[::2]:  # Every second ad
            if self.limit > 0:
                yield scrapy.Request(ad_url, callback=self.parse_ad, headers=self.headers)

    def parse_ad(self, response):
        item = ApartmentsItem()
        raw_json = response.xpath('/html/body/script[@id="server-app-state"]/text()').get()
        if raw_json:
            json_data = json.loads(raw_json).get('initialProps').get('data').get('advert')
            item['title'] = json_data['title']
            item['location'] = json_data['location']['address']
            item['price'] = json_data['price']['human_value'] + json_data['price'].get('suffix', '')  # Some categories have no suffix
            item['characteristics'] = json.dumps(json_data['characteristics'])
            item['timestamp'] = datetime.utcnow().isoformat(sep=' ', timespec='seconds')
            self.limit -= 1
            yield item
