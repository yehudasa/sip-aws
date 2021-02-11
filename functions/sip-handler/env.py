import logging
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Params:
   def __init__(self):
        self.db_prefix = os.environ.get('DB_PREFIX', '')
        self.obj_order_table = self.db_prefix + os.environ.get('DYNAMODB_ORDER_TABLE', 's3_obj_order')

        self.datalog_window_size = int(os.environ.get('DATALOG_WINDOW_SIZE', '30'))
        self.datalog_num_shards = int(os.environ.get('DATALOG_NUM_SHARDS', '16'))

        self.bilog_num_shards = int(os.environ.get('BILOG_NUM_SHARDS', '16'))
        
        self.top_level_bucket = os.environ.get('TOP_LEVEL_BUCKET', 'sip-default-bucket')


env_params = Params()
