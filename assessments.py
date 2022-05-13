"""
Exploration of real estate assessments
"""

from logging import FileHandler
from logging import INFO
from logging import StreamHandler
from logging import basicConfig
from logging import getLogger
from os.path import basename
from pathlib import Path
from sys import stdout

from arrow import now
from pandas import set_option
from requests import get

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FORMAT = '%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(message)s'
LOG_PATH = Path('./logs/')
LOG_PATH.mkdir(exist_ok=True)

if __name__ == '__main__':
    time_start = now()

    set_option('display.max_colwidth', None)  # was -1 and caused a warning
    run_start_time = now().strftime('%Y-%m-%d_%H-%M-%S')
    file_root_name = basename(__file__).replace('.py', '')
    LOGFILE = str(LOG_PATH / 'log-{}-{}.log'.format(run_start_time, file_root_name))

    handlers = [FileHandler(LOGFILE), StreamHandler(stdout)]
    # noinspection PyArgumentList
    basicConfig(datefmt=DATE_FORMAT, format=LOG_FORMAT, handlers=handlers, level=INFO, )

    logger = getLogger()
    logger.info('started')

    URL = 'https://datahub-v2.arlingtonva.us/api/RealEstate/Assessment?$top=500'

    response = get(url=URL, params={})
    data = response.json()
    logger.info(data)

    logger.info('total time: {:5.2f}s'.format((now() - time_start).total_seconds()))
