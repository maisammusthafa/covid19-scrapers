#!/bin/env python3

import js2xml

from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose


def extract_chart_data(script):
    parsed = js2xml.parse(script)

    dates = parsed.xpath('//functioncall[1]//property[@name="categories"]//string/text()')
    cases = parsed.xpath('//functioncall[1]//property[@name="data"]//number/@value')

    return {
        'dates': ['2020 {}'.format(date) for date in dates],
        'cases': cases
    }


class TimeSeriesLoader(ItemLoader):
    default_output_processor = TakeFirst()

    country_in = MapCompose(lambda x: x.strip())
    confirmed_in = MapCompose(extract_chart_data)
    deaths_in = MapCompose(extract_chart_data)
    active_in = MapCompose(extract_chart_data)


class TimeSeriesItem(Item):
    def __repr__(self):
        return repr({'country': self['country']})

    country = Field()
    confirmed = Field()
    deaths = Field()
    active = Field()
