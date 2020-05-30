#!/bin/env python3

import scrapy
from ..items import TimeSeriesLoader, TimeSeriesItem


class TimeSeriesSpider(scrapy.Spider):
    name = 'worldometers.timeseries'

    custom_settings = {
        'ITEM_PIPELINES': {
           'worldometers.pipelines.TimeSeriesPipeline': 300,
        }
    }

    def start_requests(self):
        base_url = 'https://www.worldometers.info/coronavirus/'
        yield scrapy.Request(url=base_url, callback=self.parse_countries)


    def parse_countries(self, response):
        rows = response.xpath('//table[@id="main_table_countries_today"]/tbody/tr')
        urls = rows.xpath('./td/a[@class="mt_a"]/@href')
        for url in urls[:]:
            yield response.follow(url=url, callback=self.parse_data)


    def parse_data(self, response):
        loader = TimeSeriesLoader(item=TimeSeriesItem(), selector=response)
        loader.add_xpath('country', '//div[contains(@class, "content-inner")]/div/h1/text()[2]')
        loader.add_xpath('confirmed', '//script[contains(text(), "coronavirus-cases-linear")]/text()')
        loader.add_xpath('deaths', '//script[contains(text(), "coronavirus-deaths-linear")]/text()')
        loader.add_xpath('active', '//script[contains(text(), "graph-active-cases-total")]/text()')

        yield loader.load_item()
