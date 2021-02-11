import time

from env import *
from db import *

from tools import ceph_str_hash_linux

def datalog_table_settings_cb():
    return {
        'KeySchema': [
            {
                'AttributeName': 'shard_id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'entry_id',
                'KeyType': 'RANGE'  # Sort Key
            }            
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'shard_id',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'entry_id',
                'AttributeType': 'S'
            }
        ]
    }


class DataLog:
    def __init__(self, bucket, bucket_shard_id, bucket_num_shards, timestamp):
        self.bucket = bucket
        self.bucket_shard_id = bucket_shard_id
        self.bucket_num_shards = bucket_num_shards
        self.timestamp = timestamp

        self.shard_id = ceph_str_hash_linux(bucket) % env_params.datalog_num_shards

        self.table_name = env_params.db_prefix + 'datalog'

        self.dbtable = get_table(self.table_name, datalog_table_settings_cb)
        logger.info("Table %s status: %s" % (self.table_name, self.dbtable.table_status))

    def store_entry(self, datalog_key):
        key = { 'shard_id': self.shard_id,
                'entry_id': datalog_key }
    
        try:
            self.dbtable.update_item(
                    Key=key,
                    UpdateExpression='SET #bucket=:bucket, #sid=:sid, #num_shards=:num_shards, #ts=:ts',
                    ConditionExpression='attribute_not_exists(#ts) or (#ts > :ts)', # only update if not exists new timestamp is lower (to reflect older change)
                    ExpressionAttributeNames={
                        '#bucket': 'bucket',
                        '#sid': 'bucket_shard_id',
                        '#num_shards': 'num_shards',
                        '#ts': 'timestamp',
                        },
                    ExpressionAttributeValues={
                        ':bucket': self.bucket,
                        ':sid': self.bucket_shard_id,
                        ':num_shards': self.bucket_num_shards,
                        ':ts': self.timestamp,
                    } )
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.info('datalog, condition failed (key=%s bucket=%s:%d/%d): already updated'  % (datalog_key, self.bucket, self.bucket_shard_id, self.bucket_num_shards))
            return False                    
        except BaseException as e:
            logger.error('ERROR: failed to update db (key=%s bucket=%s:%d/%d)'  % (datalog_key, self.bucket, self.bucket_shard_id, self.bucket_num_shards))
            raise e
    
        return True

    def store_entries(self):
        ts_period = env_params.datalog_window_size

        ts_half = ts_period / 2

        t = time.time()

        for delta in (ts_half, ts_period):
            ts = int(t + delta - t % ts_half)

            key = hex(ts).lstrip('0x').zfill(16) + '_' + self.bucket + '_' + str(self.bucket_shard_id)

            self.store_entry(key)

