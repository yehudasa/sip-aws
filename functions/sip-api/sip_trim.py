
import boto3

from boto3.dynamodb.conditions import Key

from db import *

def marker_table_settings_cb():
    return {
        'KeySchema': [
            {
                'AttributeName': 'source_id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'target_id',
                'KeyType': 'RANGE'  # Partition key
            },
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'source_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'target_id',
                'AttributeType': 'S'
            },
        ]
    }

class SIPMarkerHandler:
    def __init__(self, env, sip, instance, stage_id, shard_id):
        self.env = env
        self.sip = sip
        self.stage_id = stage_id
        self.instance = instance
        self.shard_id = shard_id

        self.source_id = sip + '/' + stage_id
        if instance:
            self.source_id += '/' + instance
        self.source_id += ':' + str(shard_id)

        self.table_name = env_params.db_prefix + 'target-info'
        self.dbtable = get_table(self.table_name, marker_table_settings_cb)
        logger.info("Table %s status: %s" % (self.table_name, self.dbtable.table_status))

    def get_key(self, target_id):
        return { 'source_id': self.source_id,
                 'target_id': target_id }
    
    def store_entry(self, key, marker, mtime):
                  
        logger.info("Table %s status: %s" % (self.table_name, self.dbtable.table_status))
    
        try:
            self.dbtable.update_item(
                    Key=key,
                    UpdateExpression='SET #marker=:marker, #mtime=:mtime',
                    ExpressionAttributeNames={
                        '#marker': 'marker',
                        '#mtime': 'mtime',
                        },
                    ExpressionAttributeValues={
                        ':marker': marker,
                        ':mtime': mtime,
                    } )
        except BaseException as e:
            logger.error('ERROR: failed to update db (key=%s marker=%s): %s'  % (key, marker, e))
            raise e
            
        return

    def read_min_marker(self):
        
        target_id = None
        max_entries = 1
        
        min_marker = None
        
        maybe_more = True
        
        while maybe_more:

            cond = Key('source_id').eq(self.source_id)
        
            if target_id:
                cond &= Key('target_id').gt(target_id)
                
            response = self.dbtable.query(KeyConditionExpression=cond,
                                          ScanIndexForward=True, Limit=max_entries)

            count = response['Count']
            logger.info("len: %s response: %s" % (count, response))
    
            for item in response['Items']:
                target_id = item['target_id']
                marker = item['marker']
                mtime = item['mtime']
                
                if not min_marker or marker < min_marker:
                    min_marker = marker
                    
            maybe_more = (count == max_entries)


        logger.info("min_marker: %s" % (min_marker or '<none>'))

        return min_marker
            


    def set_target_info(self, target_id, marker, mtime, check_exists):
        try:
            self.store_entry(self.get_key(target_id), marker, mtime)
            self.read_min_marker()

        except:
            # return (500, {})
            raise

        return (200, {})

