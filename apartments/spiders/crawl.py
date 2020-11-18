# -*- coding: utf-8 -*-
import scrapy
import json
from datetime import datetime
from apartments.items import ApartmentsItem


class CrawlSpider(scrapy.Spider):
    name = 'spider'
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

    def __init__(self, category, page=1, limit=1000, *args, **kwargs):
        super(CrawlSpider, self).__init__(*args, **kwargs)
        self.page = int(page)
        self.limit = int(limit)
        categories = {
            'a': 'sprzedaz/mieszkanie',
            'b': 'wynajem/mieszkanie',
            'c': 'sprzedaz/dom',
            'd': 'wynajem/dom',
        }
        self.category = categories[category]

    def start_requests(self):
        start_url = f'https://www.otodom.pl/{self.category}/?page={self.page}'
        yield scrapy.Request(start_url, headers=self.headers)

    def parse(self, response):
        ad_urls = response.xpath('//div[@class="offer-item-details"]/header/h3/a/@href').getall()
        for ad_url in ad_urls[::2]:  # Every second ad
            if self.limit > 0:
                yield scrapy.Request(ad_url, callback=self.parse_ad, headers=self.headers)
                self.limit -= 1

        # Pagination
        next_page_url = response.xpath('//li[@class="pager-next"]/a/@href').get()
        if next_page_url and self.limit > 0:
            print(next_page_url)
            yield scrapy.Request(next_page_url, callback=self.parse, headers=self.headers)
            pass

    def parse_ad(self, response):
        item = ApartmentsItem()
        raw_data = response.xpath('/html/body/script[@id="server-app-state"]/text()').get()
        if raw_data:
            json_data = json.loads(raw_data).get('initialProps')
            item['title'] = json_data['data']['advert']['title']
            item['json'] = json.dumps(json_data)
            item['timestamp'] = datetime.utcnow().isoformat(sep=' ', timespec='seconds')
            yield item
