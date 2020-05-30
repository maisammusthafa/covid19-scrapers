#!/bin/env python3

import pandas as pd
from functools import reduce


class TimeSeriesPipeline:
    def __init__(self):
        self.dfs = {
            'confirmed': pd.DataFrame(),
            'deaths': pd.DataFrame(),
            'active': pd.DataFrame(),
        }

        self.dfs_diff = {}


    def process_item(self, item, spider):
        for category in self.dfs:
            series = pd.Series([], name=category, dtype='float64')

            if category in item:
                index = pd.to_datetime(item[category]['dates'], format='%Y %b %d')
                data = item[category]['cases']
                series = pd.Series(data=data, index=index, name=category, dtype='float64')

            df = series.to_frame(name=item['country'])

            if self.dfs[category].empty:
                self.dfs[category] = df
            else:
                self.dfs[category] = self.dfs[category].join(df, how='outer')

        return item


    def close_spider(self, spider):
        self.dfs['recovered'] = self.dfs['confirmed'] - self.dfs['deaths'] - self.dfs['active']

        for category in self.dfs:
            self.dfs[category].sort_index(axis=1, inplace=True)
            self.dfs_diff[category] = self.dfs[category].diff()

        self.dump_to_csv('data/covid-19_time_series.csv', spider.logger)
        self.dump_to_excel('data/covid-19_time_series_total.xlsx', 'total', spider.logger)
        self.dump_to_excel('data/covid-19_time_series_diff.xlsx', 'diff', spider.logger)


    def dump_to_csv(self, file_name, logger):
        dfs = {}

        for category in self.dfs:
            dfs[category] = self.dfs[category].reset_index().rename(columns={'index': 'date'}).melt(
                id_vars='date', var_name='country', value_name=category
            )

        result = reduce(lambda left, right: pd.merge(
            left, right, how='outer', on=['date', 'country']), dfs.values()
        )

        result.to_csv(file_name, float_format='%0.0f', index=False)
        logger.info('Dumped COVID-19 time series data (long format) to {0}\n{1}'.format(file_name, result))


    def dump_to_excel(self, file_name, case_type, logger):
        dfs = self.dfs if case_type == 'total' else self.dfs_diff

        with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
            for category, df in dfs.items():
                df.index = df.index.date
                df.index.name = 'Date'
                df.reset_index(inplace=True)
                df.to_excel(writer, sheet_name=category.capitalize(), index=False)

            for sheet in writer.sheets.values():
                sheet.set_column('A:A', 12)
                sheet.freeze_panes(1, 1)

        writer.save()
        logger.info('Dumped COVID-19 time series data ({0}) to {1}'.format(case_type, file_name))
