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
from pandas import set_option

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FORMAT = '%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(message)s'
LOG_PATH = Path('./logs/')

if __name__ == '__main__':
    time_start = now()
    LOG_PATH.mkdir(exist_ok=True)

    set_option('display.max_colwidth', None)  # was -1 and caused a warning
    run_start_time = now().strftime('%Y-%m-%d_%H-%M-%S')
    file_root_name = 'find_property'
    LOGFILE = '{}/log-{}-{}.log'.format(LOG_PATH, run_start_time, file_root_name)

    handlers = [FileHandler(LOGFILE), StreamHandler(stdout)]
    # noinspection PyArgumentList
    basicConfig(datefmt=DATE_FORMAT, format=LOG_FORMAT, handlers=handlers, level=INFO, )

    logger = getLogger()
    logger.info('started')

    logger.info('total time: {:5.2f}s'.format((now() - time_start).total_seconds()))
