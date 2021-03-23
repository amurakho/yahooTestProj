import yfinance as yf
import numpy as np
from datetime import timedelta
import logging
import requests
import bs4
import re
import pandas as pd
import json
from pprint import pprint

from const import COMPANIES

logging.basicConfig(level=logging.INFO)


class NewsParser:
    base_url = 'https://finance.yahoo.com/quote/{}'

    def __init__(self, save=True):
        self.save = save

    def pass_request(self, company_name):
        url = self.base_url.format(company_name)
        # turn off redirect, because yahoo automatically redirect if the company name are wrong
        response = requests.get(url, allow_redirects=False)

        if response.status_code == 302:
            return None

        return response.content.decode()

    def content_to_json(self, content):
        """
            I could parse html, but I decided not to do it because the markup may change but script no(less chance)

            So I spend more time to analyze and maybe write not clean enough code, but I think this decision is more
            stable
        """
        #
        soup = bs4.BeautifulSoup(content, 'lxml')

        scripts = soup.find_all('script')
        # get needed script
        for script in scripts:

            # take all between stream_items and more_items variables
            if s := (re.search(r'"stream_items":(.*?),"more_items"', str(script.string))):
                needed_script = s.group(1)
                logging.info('Needed script are found')
                break
        else:
            logging.error("Can't find needed script!")
            return
        data = json.loads(needed_script)
        return data

    def parse_json(self, data):
        cleaned_data = pd.DataFrame(columns=['link', 'title'])
        for row in data:
            cleaned_data = cleaned_data.append({
                'link': row.get('url'),
                'title': row.get('title')
            }, ignore_index=True)
        logging.info(f'Date are cleaned - first {cleaned_data.shape[0]} rows')
        return cleaned_data

    def manage(self, company_name):
        content = self.pass_request(company_name)

        if not content:
            logging.error('Wrong company name - have no data. This cycle is continued')
            return

        data = self.content_to_json(content)
        cleaned_data = self.parse_json(data)

        if self.save:
            cleaned_data.to_csv(f'News{company_name}.csv')

class DataEngine:

    def __init__(self, save=True):
        self.company_name = ''
        self.save = save

    def get_the_data(self, company_name, period='max'):
        """
        Get company name  and pass request by yfinance's Ticker
        reset index and return data

        """
        # get access to company
        tiker = yf.Ticker(self.company_name)

        # get info from company: auto_adjust -> Turn off adjust auto
        # and sorting
        hist = tiker.history(period=period, auto_adjust=False).sort_values(by=['Date'], ascending=False)

        # by default date column are index, so i change it
        hist = hist.reset_index()

        return hist

    def create_three_day_before_column(self, data):
        """
        Create new column and return updated data
        """

        #  will use for splitting the data by date(every 3 days)
        next_date_should_be = data.iloc[0]['Date']

        def make_new(x):
            """
                get local next_date_should_be variable which will contain what date should be next

                i compare date in each row with next_date_should_be
                if new date are less or equal - i calculate new next date
                and return date
                else
                return None

                unfortunately, I cannot calculate 3day_before change here because when I calculate what date should be next.
                I actually don't know is there a row with the given date

            """
            nonlocal next_date_should_be

            date = x['Date']
            if date <= next_date_should_be:
                next_date_should_be = date - timedelta(days=3)
                return x
            else:
                return np.nan

        # drop nan and take only few columns
        three_day_before_df = data.apply(make_new, axis=1).dropna()[['Date', 'Adj Close']]

        # calculate new value
        new_column = three_day_before_df[['Adj Close']].shift() / three_day_before_df[['Adj Close']]

        three_day_before_df['3day_before_change'] = new_column.shift(-1)

        return data.merge(three_day_before_df, how='left')

    def manage(self, company_name):

        self.company_name = company_name

        logging.info(f'Start working with "{self.company_name}" company')

        data = self.get_the_data(self.company_name)
        if data.empty:
            logging.error('Wrong company name - have no data. This cycle is continued')
            return
        else:
            logging.info(f'Get data with {data.shape[0]} rows')
        updated_data = self.create_three_day_before_column(data)
        logging.info('Data was changed')

        if self.save:
            updated_data.to_csv(self.company_name + '.csv')
            logging.info('Data was saved')


def main():
    pass
    # parser = NewsParser()
    # engine = DataEngine()
    #
    # for company in COMPANIES:
    #     engine.manage(company)
    #
    #     parser.manage(company)
    #     logging.info(f'Finish work with {company}')


if __name__ == '__main__':
    main()
