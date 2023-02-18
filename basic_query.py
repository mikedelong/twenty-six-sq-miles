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

from arrow import now
from bs4 import BeautifulSoup
from pandas import set_option
from requests import get

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FORMAT = '%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(message)s'
LOG_PATH = Path('./logs/')
OUTPUT_FOLDER = './data/'
URL = 'https://propertysearch.arlingtonva.us/Home/GeneralInformation?lrsn=17540'

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

    for folder in [OUTPUT_FOLDER]:
        logger.info('creating folder %s if it does not exist', folder)
        Path(folder).mkdir(parents=True, exist_ok=True)

    result = get(url=URL)

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
    result = dict()
    for index, item in enumerate(subdivs):
        pieces = item.text.split('\n')
        pieces = [' '.join(piece.split()) for piece in pieces]
        pieces = [piece for piece in pieces if piece]
        if pieces:
            logger.info('%d %s', index, pieces)
        if index == 1:
            result['RPC'] = pieces[0]
        elif index == 2:
            result['Address'] = pieces[0]
        elif index in {4, 5, 9, 10, 11, 13, 15, 16, 18, 19, 20, 22, 23, 24}:
            result[pieces[0]] = pieces[1]
        elif index == 6:
            result[pieces[0]] = ' '.join(pieces[1:])
    logger.info(result)
    logger.info('total time: {:5.2f}s'.format((now() - time_start).total_seconds()))
