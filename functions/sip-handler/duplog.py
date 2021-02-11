from env import *
from db import *

from tools import ceph_str_hash_linux


def dup_table_settings_cb():
    return {
        'KeySchema': [
            {
                'AttributeName': 'bucket',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'obj',
                'KeyType': 'RANGE'  # Sort Key
            }
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'bucket',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'obj',
                'AttributeType': 'S'
            }
        ]
    }

class DupLog:
    def __init__(self):
        self.dbtable = get_table(env_params.obj_order_table, dup_table_settings_cb)
        logger.info("Table %s status: %s" % (env_params.obj_order_table, self.dbtable.table_status))
    
        
    def check_seq(self, bucket, obj_key, obj_seq):
        key = { 'bucket': bucket,
                'obj': obj_key }
    
        try:
            self.dbtable.update_item(
                    Key=key,
                    UpdateExpression='SET #seq=:seq',
                    ConditionExpression='attribute_not_exists(#seq) or (#seq <= :seq)', # FIXME: should be only <
                    ExpressionAttributeNames={
                        '#seq': 'obj_seq',
                        },
                    ExpressionAttributeValues={
                        ':seq': obj_seq,
                        } )
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.info('condition failed on obj (%s/%s) for newer sequence (%d): skipping' % (bucket, obj_key, obj_seq))
            return False
        except BaseException as e:
            logger.error('ERROR: failed to update db (obj=%s/%s, seq=%d): %s'  % (bucket, obj_key, obj_seq, e))
            raise e
    
        return True
