"""
Exploration of real estate assessments
"""

from logging import FileHandler
from logging import INFO
from logging import StreamHandler
from logging import basicConfig
from logging import getLogger
from pathlib import Path
from sys import stdout
from time import sleep

from arrow import now
from bs4 import BeautifulSoup
from pandas import DataFrame
from pandas import concat
from pandas import read_csv
from pandas import set_option
from requests import get

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FORMAT = '%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(message)s'
LOG_PATH = Path('./logs/')
ORIGINAL = 17540
OUTPUT_FILE = 'df.csv'
OUTPUT_FOLDER = './data/'
URL = 'https://propertysearch.arlingtonva.us/Home/GeneralInformation?lrsn={}'
USECOLS = ['LRSN', 'RPC', 'Address', 'Owner',
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

    prior_df = read_csv(filepath_or_buffer=output_file, usecols=USECOLS)

    documents = list()
    for lrsn in range(16193, 22000):
        if lrsn not in prior_df['LRSN'].values:
            sleep(1)
            url = URL.format(lrsn)
            logger.info(url)
            result = get(url=url)

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
            documents.append(document)
            if lrsn % 10 == 0:
                df = DataFrame(data=documents).drop_duplicates()
                result_df = concat([df, prior_df]).drop_duplicates(ignore_index=True)
                logger.info('writing %d records to %s', len(result_df), output_file)
                result_df.to_csv(path_or_buf=output_file, index=False)
    df = DataFrame(data=documents)
    result_df = concat([df, prior_df]).drop_duplicates(ignore_index=True)
    result_df = result_df.sort_values(by='LRSN')
    logger.info('writing %d records to %s', len(result_df), output_file)
    result_df.to_csv(path_or_buf=output_file, index=False)

    logger.info('total time: {:5.2f}s'.format((now() - time_start).total_seconds()))
