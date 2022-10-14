#!python3

from ctypes.wintypes import INT
from itertools import count
from mrjob.job import MRJob
from mrjob.step import MRStep

import csv
import json

cols = 'id_customer,name_customer,birthdate_customer,gender_customer,country_customer,date_transaction,product,product_transaction,amount_transaction'.split(
    ',')


def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row


class AmountSum(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort)
        ]

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, csv_readline(line)))

        # skip first row as header
        if row['id_customer'] != 'id_customer':
            # Yield the order_date
            yield row['date_transaction'][0:4], bigint(row['amount_transaction'])

    def reducer(self, key, values):
        # for 'order_date' compute
        yield None, (key, sum(values))

    def sort(self, key, values):
        data = []
        for date_transaction, amount_total in values:
            data.append((date_transaction, amount_total))
            data.sort()
        for date_transaction, amount_total in data:
            yield date_transaction, amount_total


if __name__ == '__main__':
    AmountSum.run()
