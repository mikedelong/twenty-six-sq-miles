"""
Exploration of real estate assessments
"""

from logging import FileHandler
from logging import INFO
from logging import StreamHandler
from logging import basicConfig
from logging import getLogger
from pathlib import Path
from random import random
from sys import stdout
from time import sleep

from arrow import now
from bs4 import BeautifulSoup
from pandas import DataFrame
from pandas import concat
from pandas import read_csv
from pandas import set_option
from requests import get
from requests.exceptions import ConnectionError
from requests.exceptions import ReadTimeout

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DTYPES = {
    'RPC': object,
    'Address': object,
    'Owner': object,
    'Legal Description': object,
    'Mailing Address': object,
    'Year Built': float,
    'Units': float,
    'EU#': object,
    'Property Class Code': object,
    'Zoning': object,
    'Lot Size': float,
    'Neighborhood#': int,
    'Map Book/Page': object,
    'Polygon': object,
    'Site Plan': object,
    'Rezoning': float,
    'Tax Exempt': object,
    'LRSN': int,
    'fetched': object,
    'Condo Unit': object,
    'Condo Model': object,
    'Additional Owners': object,
    'GFA': float,
    'Trade Name': object,
}
LOG_FORMAT = '%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(message)s'
LOG_PATH = Path('./logs/')
OUTPUT_FILE = 'df.csv'
OUTPUT_FOLDER = './data/'
SKIP = {381, 2782, 2791, 4287, 6056, 6909, 7094, 18766, 30354, 31769, 36067, 36454, 42235, 42236, 44302, 45654, 45655,
        45880, 46640, 46641, 46660, 46661, 46670, 47581, 47584, 47591, }
URL = 'https://propertysearch.arlingtonva.us/Home/GeneralInformation?lrsn={:05d}'
USECOLS = ['LRSN', 'fetched', 'RPC', 'Address', 'Owner',
           'Legal Description', 'Mailing Address', 'Year Built', 'Units', 'EU#',
           'Property Class Code', 'Zoning', 'Lot Size', 'Neighborhood#',
           'Map Book/Page', 'Polygon', 'Site Plan', 'Rezoning', 'Tax Exempt',
           'Additional Owners', 'Trade Name', 'GFA', 'Condo Unit', 'Condo Model']

if __name__ == '__main__':
    time_start = now()
    LOG_PATH.mkdir(exist_ok=True)

    set_option('display.max_colwidth', None)  # was -1 and caused a warning
    run_start_time = now().strftime('%Y-%m-%d_%H-%M-%S')
    file_root_name = 'basic_query'
    LOGFILE = '{}/log-{}-{}.log'.format(LOG_PATH, run_start_time, file_root_name)

    handlers = [FileHandler(LOGFILE), StreamHandler(stdout)]
    # noinspection PyArgumentList
    basicConfig(datefmt=DATE_FORMAT, format=LOG_FORMAT, handlers=handlers, level=INFO, )

    logger = getLogger()
    logger.info('started')

    output_file = OUTPUT_FOLDER + OUTPUT_FILE
    for folder in [OUTPUT_FOLDER]:
        logger.info('creating folder %s if it does not exist', folder)
        Path(folder).mkdir(parents=True, exist_ok=True)

    prior_df = read_csv(dtype=DTYPES, filepath_or_buffer=output_file, usecols=USECOLS, )

    documents = list()
    count = 0
    for lrsn in range(132, 50000):
        do_case = (lrsn not in SKIP)
        do_case &= lrsn not in prior_df['LRSN'].values
        if lrsn in prior_df['LRSN'].values:
            lrsn_df = prior_df[prior_df['LRSN'] == lrsn]
            do_case &= lrsn_df['fetched'].isna().sum() == 1
        if do_case:
            url = URL.format(lrsn)
            logger.info(url)

            success = False
            sleep_period = random()
            result = None
            while not success:
                sleep(sleep_period)
                try:
                    result = get(url=url)
                    success = True
                except ConnectionError as error:
                    sleep_period *= 2.0
                    logger.warning('connection error: sleeping %0.2f', sleep_period)
                except ReadTimeout as error:
                    sleep_period *= 2.0
                    logger.warning('read timeout: sleeping %0.2f', sleep_period)

            soup = BeautifulSoup(result.text, 'html.parser')
            body = soup.find('body')
            site_inner = body.find_all('div', {'class': 'site-inner'})[0]
            wrapper = site_inner.find('div', {'class': 'content-sidebar-wrap'})
            main = wrapper.find('main')
            article = main.find('article')
            entry_content = article.find('div', {'class': 'entry-content'})
            divs = entry_content.find_all('div')
            subs = divs[3].find_all('div')
            subdivs = subs[0].find_all('div')
            document = dict()
            if subdivs[3].text == '(Inactive)':
                for index, item in enumerate(subdivs):
                    pieces = item.text.split('\n')
                    pieces = [' '.join(piece.split()) for piece in pieces]
                    pieces = [piece for piece in pieces if piece]
                    if pieces:
                        if index == 1:
                            document['RPC'] = pieces[0]
                        elif index == 2:
                            document['Address'] = pieces[0]
                        elif index in {5, 6, 11, 12, 13, 15, 17, 18, 20, 21, 22, 24, 25, 26}:
                            document[pieces[0]] = pieces[1]
                        elif index == 8:
                            document[pieces[0]] = ' '.join(pieces[1:])
                        elif index == 9:
                            if len(pieces) == 1:
                                document[pieces[0]] = ''
                            else:
                                document[pieces[0]] = pieces[1]
            else:
                for index, item in enumerate(subdivs):
                    pieces = item.text.split('\n')
                    pieces = [' '.join(piece.split()) for piece in pieces]
                    pieces = [piece for piece in pieces if piece]
                    if pieces:
                        if index == 1:
                            document['RPC'] = pieces[0]
                        elif index == 2:
                            document['Address'] = pieces[0]
                        elif index in {4, 5, 9, 10, 11, 13, 15, 16, 18, 19, 20, 22, 23, 24}:
                            document[pieces[0]] = pieces[1]
                        elif index == 6:
                            document[pieces[0]] = ' '.join(pieces[1:])
            document['LRSN'] = lrsn
            document['fetched'] = now()
            if lrsn in prior_df['LRSN'].values:
                prior_df = prior_df[prior_df['LRSN'] != lrsn]
                logger.info('dropping LRSN %d from prior', lrsn)
            documents.append(document)
            count += 1
            if count % 10 == 0:
                df = DataFrame(data=documents).drop_duplicates()
                result_df = concat([df, prior_df]).drop_duplicates(ignore_index=True)
                logger.info('writing %d records to %s', len(result_df), output_file)
                result_df.to_csv(path_or_buf=output_file, index=False)
    df = DataFrame(data=documents)
    result_df = concat([df, prior_df]).drop_duplicates(ignore_index=True)
    result_df = result_df.sort_values(by='LRSN').reset_index()
    logger.info('writing %d records to %s', len(result_df), output_file)
    result_df.to_csv(path_or_buf=output_file, index=False)

    logger.info('total time: {:5.2f}s'.format((now() - time_start).total_seconds()))
