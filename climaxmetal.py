# -*- coding: utf-8 -*-
"""
Python 3.5.4
This code scrapes the products from the site "http://store.climaxmetal.com/"
and stores them in the file "Result.cssv"
Example: python python climaxmetal.py
If you pass the key "-m" when running the code,
the code will be executed in two threads.
If you pass an integer after the key "-m", then
the code will be executed in as many threads as you pass after the key "-m".
Example: python python climaxmetal.py -m5
"""


import requests
import re
import sys
import csv
import os
import threading

from lxml import html
from datetime import datetime


class ClimaxScraper(object):
    USERNAME = 'mechdrives'
    PASSWORD = 'MEC002'
    URL = 'http://store.climaxmetal.com/'
    USERAGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' \
                '(KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36'
    PRODUCT_DETAILS_URL = 'http://store.climaxmetal.com/ProductDetails.aspx?'
    PRODUCT_LISTING_URL = 'http://store.climaxmetal.com/ProductListing.aspx?'
    fields = [
        'Item',
        'Category',
        'Title',
        'Specification',
        'Price',
        'Image'
    ]

    def __init__(self):
        self._session = None
        self.file = r'{}\Result.csv'.format(os.getcwd()).replace('\\', '/')

    @staticmethod
    def get_file_name(file):
        return r'{0}\{1}.csv'.format(os.getcwd(), file).replace('\\', '/')

    @property
    def session(self):
        if self._session is None:
            self._session = requests.Session()
            self._session.verify = False
            self._session.headers = {'User-Agent': self.USERAGENT}
        return self._session

    def get(self, url, **kwargs):
        resp = self.session.get(url, **kwargs)
        return resp

    def get_x(self, url, **kwargs):
        kwargs.setdefault('timeout', 60)
        page = self.get(url, **kwargs)
        if page.status_code == 200:
            x = html.fromstring(page.content)
            x.make_links_absolute(url)
            return x
        return None

    def post(self, url, data, *args, **kwargs):
        kwargs.setdefault('timeout', 60)
        resp = self.session.post(url, data, *args, **kwargs)
        return resp

    def post_x(self, url, data, *args, **kwargs):
        page = self.post(url, data, *args, **kwargs)
        if page.status_code == 200:
            x = html.fromstring(page.content)
            x.make_links_absolute(url)
            return x

    @staticmethod
    def get_form_data(x, form_xpath):
        res = {}
        for el in x.xpath(form_xpath + "//input"):
            name = el.attrib.get('name')
            if name:
                res[name] = el.attrib.get('value', '')
        return res

    def clean_price(self, price):
        price = self.get_first(price)
        if u',' in price and '.' in price:
            price = price.replace(',', '')
        price = price.replace(' ', '')\
            .replace('$', '') \
            .replace('USD', '').strip()
        try:
            return float(price)
        except ValueError:
            return 0

    @staticmethod
    def get_first(lst):
        try:
            return lst[0] if isinstance(lst, list) else lst
        except IndexError:
            return ''

    @staticmethod
    def get_spec(parsed_page):
        specification = []
        keys = parsed_page.xpath(
            '//table[@class="specification-table"]/tr/td/strong/text()')
        values = parsed_page.xpath(
            '//table[@class="specification-table"]/tr/td/text()')
        for index, key in enumerate(keys):
            specification.append(key + values[index])
        return specification

    @staticmethod
    def get_from_csv(filename, no_file=False):
        try:
            with open(filename, "r") as f:
                rows = csv.DictReader(f)
                for row in rows:
                    yield row
        except (IOError, OSError) as err:
            if not ('No such file or directory' in str(err) and no_file):
                print('WARNING: {}'.format(err))

    def save_to_csv(self, results, file_open, file_name=None):
        out_file = file_name if file_name else self.file
        if results:
            try:
                if not file_open:
                    with open(out_file, 'w', newline='') as csv_f:
                        writer = csv.DictWriter(csv_f, fieldnames=self.fields)
                        writer.writeheader()
                with open(out_file, 'a', newline='') as csv_f:
                    for result in results:
                        writer = csv.DictWriter(csv_f, fieldnames=self.fields)
                        writer.writerow(result)
            except (IOError, OSError) as err:
                print('ERROR save csv: {}'.format(err))

    def process(self, categories, response, file_name=None):
        file_open = False
        for category in categories:
            results = []
            parsed_page = self.post_x(category, response)
            answer = parsed_page.xpath('//a/@href')
            name_category = self.get_first(parsed_page.xpath(
                '//div[@class="span9"]/h2/text()'))
            for link in answer:
                if link.startswith(self.PRODUCT_DETAILS_URL):
                    parsed_page = self.post_x(link, response)
                    item = self.get_first(parsed_page.xpath(
                        '//p[@class="product-item-number"]/strong/text()'))
                    title = self.get_first(parsed_page.xpath(
                        '//h2[@class="product-title"]/text()'))
                    specification = self.get_spec(parsed_page)
                    price = self.clean_price(parsed_page.xpath(
                        '//span[@id="Cus_Price"]/span/text()'))
                    image = self.get_first(parsed_page.xpath(
                        '//img[@id="productimage"]/@src'))
                    item_dict = {
                        self.fields[0]: item,
                        self.fields[1]: name_category,
                        self.fields[2]: re.sub(r'\s+', ' ', title),
                        self.fields[3]: specification,
                        self.fields[4]: price,
                        self.fields[5]: image
                    }
                    results.append(item_dict)
            self.save_to_csv(results, file_open, file_name)
            file_open = True
            print('Scraping items of category {} done'.format(name_category))

    def get_scraping(self, multi=None):
        start_time = datetime.now()

        # Login
        parsed_page = self.get_x(self.URL)
        response = self.get_form_data(parsed_page, "")
        response['ctl00$cntMain$TXTGLOBALUSERNAME3'] = self.USERNAME
        response['ctl00$cntMain$TXTGLOBALPASSWORD2'] = self.PASSWORD
        parsed_page = self.post_x(self.URL, response)
        array = parsed_page.xpath('//a/@href')

        categories = []
        for selected in array:
            if selected.startswith(self.PRODUCT_LISTING_URL):
                categories.append(selected)
        print('There are {} categories for scraping'.format(len(categories)))

        if multi:
            multi = multi if multi > 2 else 2
            threads = []
            quantity_in_thread = len(categories) // multi
            start_index, stop_index = 0, 0
            for index in range(0, multi):
                if index == multi - 1:
                    stop_index = None
                else:
                    stop_index += quantity_in_thread
                threads.append(threading.Thread(
                    target=self.process,
                    args=(
                        categories[start_index:stop_index],
                        response,
                        self.get_file_name('Result{}'.format(index))
                    )
                ))
                start_index = stop_index
            print('Scraping is doing in {} threads.'.format(len(threads)))

            for t in threads:
                t.start()
            for t in threads:
                t.join()

            file_open = False
            for index in range(0, multi):
                items = self.get_from_csv(
                    self.get_file_name('Result{}'.format(index)))
                self.save_to_csv(items, file_open)
                file_open = True

        else:
            self.process(categories, response)

        print('Duration: {}'.format(datetime.now() - start_time))


if __name__ == '__main__':
    climaxmetal = ClimaxScraper()

    if len(sys.argv) == 2 and sys.argv[1] == '-m':
        climaxmetal.get_scraping(2)
    elif len(sys.argv) == 2 and sys.argv[1].startswith('-m'):
        number = sys.argv[1].strip('-m')
        try:
            climaxmetal.get_scraping(int(number))
        except ValueError:
            print('After the key "-m" must be an integer number of threads')
    elif len(sys.argv) == 1:
        climaxmetal.get_scraping()
    else:
        print("Incorrect program launch with keys")
