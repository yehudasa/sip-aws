import logging
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Params:
   def __init__(self):
        self.obj_order_table = os.environ.get('DYNAMODB_ORDER_TABLE') or 's3_obj_order'

        self.datalog_window_size = int(os.environ.get('DATALOG_WINDOW_SIZE') or '30')
        self.datalog_num_shards = int(os.environ.get('DATALOG_NUM_SHARDS') or '16')

        self.bilog_num_shards = int(os.environ.get('BILOG_NUM_SHARDS') or '16')

        self.db_prefix = os.environ.get('DB_PREFIX') or ''
        
        
env_params = Params()
