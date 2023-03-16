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

CURRENT = {
    30354,
}
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
FALSE = False
INACTIVE_SPECIAL_CASES = {2782, 2791, 4287, 6056, 6909, 7094, }
LOG_FORMAT = '%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(message)s'
LOG_PATH = Path('./logs/')
OUTPUT_FILE = 'df.csv'
OUTPUT_FOLDER = './data/'
SKIP = {
    18766,
    31769, 36067, 36454, 42235, 42236, 44302, 45654, 45655, 45880, 46640, 46641, 46660, 46661, 46670,
    47581, 47584, 47591, 47731, 47975, 48279, 48562, 48564, 48577, 48579, 48587, 48589, 48597, 48599, 48610, 48613,
    48748, 48749, 48750, 48753, 48754, 48755, 48758, 48759, 48760, 48854, 48966, 48993, 49011, 49344, 49375, 49397,
    49414, 49694, 50010, 50055, 50056, 50115, 54050, 54288, 57976, 57977, 57978, 57979, 57980, 57981, 57982, 57983,
    57984, 57985, 57986, 57987, 57988, 58002, 58003, 58012, 58031, 58035, 58064, 58090, 58109, 58115, 58144, 58303,
    58578, 58798, 58799, 58822, 58824, 58825, 58827, 58828, 58830, 58831, 58833, 58834, 58915, 58932, 58962, 59002,
    59119, 59169, 59595, 59627, 59700, 59701, 59704, 59720, 59723, 59724, 59826, 59841, }

URL = 'https://propertysearch.arlingtonva.us/Home/GeneralInformation?lrsn={:05d}'
USECOLS = ['LRSN', 'fetched', 'RPC', 'Address', 'Owner',
           'Legal Description', 'Mailing Address', 'Year Built', 'Units', 'EU#',
           'Property Class Code', 'Zoning', 'Lot Size', 'Neighborhood#',
           'Map Book/Page', 'Polygon', 'Site Plan', 'Rezoning', 'Tax Exempt',
           'Additional Owners', 'Trade Name', 'GFA', 'Condo Unit', 'Condo Model', ]

if __name__ == '__main__':
    time_start = now()
    LOG_PATH.mkdir(exist_ok=True)

    set_option('display.max_colwidth', None)  # was -1 and caused a warning
    run_start_time = now().strftime('%Y-%m-%d_%H-%M-%S')
    file_root_name = 'get_property_owner_data'
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
    for lrsn in range(132, 59841):
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

            try:
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
                if FALSE:
                    pass
                elif subdivs[3].text == '(Inactive)' and lrsn not in INACTIVE_SPECIAL_CASES and lrsn not in CURRENT:
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
                elif subdivs[3].text != '(Inactive)' and lrsn not in {381, }:
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
                elif lrsn in {381, }:
                    pieces = [subitem.strip() for item in subdivs for subitem in item.text.split('\n') if
                              subitem.strip()]
                    document['RPC'] = pieces[0]
                    document['Address'] = pieces[1]
                    for index in {4, 6, 12, 22, 24, 26, 34, 36, 38, 46, 48, 50, 58, 60, 62, }:
                        document[pieces[index]] = pieces[index + 1]
                    for index in {14, }:
                        field = ' '.join([pieces[index + 1], pieces[index + 2]])
                        document[pieces[index]] = ' '.join(field.split())
                elif lrsn in INACTIVE_SPECIAL_CASES:
                    pieces = [subitem.strip() for item in subdivs for subitem in item.text.split('\n') if
                              subitem.strip()]
                    document['RPC'] = pieces[0]
                    document['Address'] = pieces[1]
                    for index in {5, 7, 19, 21, 23, 31, 33, 35, 43, 45, 47, 55, 57, 59, }:
                        document[pieces[index]] = pieces[index + 1]
                    for index in {13, }:
                        field = ' '.join([pieces[index + 1], pieces[index + 2]])
                        document[pieces[index]] = ' '.join(field.split())
                    for index in {67, }:
                        document['Note'] = pieces[index]
                elif lrsn in CURRENT:
                    pieces = [subitem.strip() for item in subdivs for subitem in item.text.split('\n') if
                              subitem.strip()]
                    document['RPC'] = pieces[0]
                    document['Address'] = pieces[1]
                    document['Legal Description'] = ''
                    document['Mailing Address'] = ' '.join(pieces[12].split() + pieces[13].split())
                    for index in {5, 17, 19, 21, 23, 29, 31, 33, 41, 43, 53, 55, 57, }:
                        document[pieces[index]] = pieces[index + 1]
                else:
                    raise NotImplementedError(lrsn)
                document['LRSN'] = lrsn
                document['fetched'] = now()
                if lrsn in prior_df['LRSN'].values:
                    prior_df = prior_df[prior_df['LRSN'] != lrsn]
                    logger.info('dropping LRSN %d from prior', lrsn)
                documents.append(document)
                count += 1
            except IndexError:
                SKIP.add(lrsn)
                logger.info(SKIP)
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
    logger.info(sorted(list(SKIP)))

    logger.info('total time: {:5.2f}s'.format((now() - time_start).total_seconds()))
