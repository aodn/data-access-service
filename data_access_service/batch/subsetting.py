import logging
from data_access_service import init_log
from data_access_service.tasks.generate_csv_file import process_csv_data_file

logger = init_log(logging.DEBUG)

def execute(job_id, parameters):
    # get params
    uuid = parameters['uuid']
    start_date = parameters['start_date']
    end_date = parameters['end_date']
    multi_polygon = parameters['multi_polygon']
    recipient = parameters['recipient']

    logger.info('UUID:', uuid)
    logger.info('Start Date:', start_date)
    logger.info('End Date:', end_date)
    logger.info('Multi Polygon:', multi_polygon)
    logger.info('Recipient:', recipient)

    process_csv_data_file(job_id, uuid, start_date, end_date, multi_polygon, recipient)