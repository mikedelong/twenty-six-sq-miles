"""
Exploration of real estate assessments
"""

from logging import FileHandler
from logging import INFO
from logging import StreamHandler
from logging import basicConfig
from logging import getLogger
from os.path import exists
from pathlib import Path
from sys import stdout

from arrow import now
from pandas import read_csv
from pandas import set_option
from pandas import to_datetime
from requests import get

COLUMNS = ['AssessmentKey', 'ProvalLrsnId', 'RealEstatePropertyCode', 'AssessmentChangeReasonTypeDsc', 'AssessmentDate',
           'ImprovementValueAmt', 'LandValueAmt', 'TotalValueAmt']
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FORMAT = '%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(message)s'
LOG_PATH = Path('./logs/')
OUTPUT_FOLDER = './data/'
URL = 'https://download.data.arlingtonva.us/RealEstate/Assessment.txt.gz?v=1676010224'

if __name__ == '__main__':
    time_start = now()
    LOG_PATH.mkdir(exist_ok=True)

    set_option('display.max_colwidth', None)  # was -1 and caused a warning
    run_start_time = now().strftime('%Y-%m-%d_%H-%M-%S')
    file_root_name = 'get_assessments'
    LOGFILE = '{}/log-{}-{}.log'.format(LOG_PATH, run_start_time, file_root_name)

    handlers = [FileHandler(LOGFILE), StreamHandler(stdout)]
    # noinspection PyArgumentList
    basicConfig(datefmt=DATE_FORMAT, format=LOG_FORMAT, handlers=handlers, level=INFO, )

    logger = getLogger()
    logger.info('started')

    for folder in [OUTPUT_FOLDER]:
        logger.info('creating folder %s if it does not exist', folder)
        Path(folder).mkdir(parents=True, exist_ok=True)

    assessments_file = OUTPUT_FOLDER + 'Assessment.txt.gz'
    if not exists(path=assessments_file):
        logger.info('downloading: %s', assessments_file)
        with open(file=assessments_file, mode='wb') as output_fp:
            response = get(url=URL, params={}, stream=True)
            output_fp.write(response.content)

    df = read_csv(filepath_or_buffer=assessments_file, compression='infer', sep='|')
    logger.info(df.shape)
    df['date'] = to_datetime(df['AssessmentDate'])
    df['year'] = df['date'].apply(lambda x: x.year)
    for column in df.columns:
        logger.info('unique count: %s %d', column, df[column].nunique())
    max_year = df['year'].max()
    logger.info('year %d has %d rows', max_year, len(df[df['year'] == max_year]))

    logger.info('total time: {:5.2f}s'.format((now() - time_start).total_seconds()))
