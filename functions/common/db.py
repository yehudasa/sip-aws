import boto3
from env import *

dynamodb = boto3.resource('dynamodb')

tables = {}

def _get_table(table_name, settings_cb):
    try:
        table = dynamodb.Table(table_name)
        status = table.table_status
        return table
    except:
        logger.info('*** dynamodb table not found (%s)' % table_name)
        pass

    logger.info('creating new dynamodb table: %s' % table_name)

    settings = settings_cb()

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=settings['KeySchema'],
            AttributeDefinitions=settings['AttributeDefinitions'],
            BillingMode='PAY_PER_REQUEST'
            #ProvisionedThroughput={
            #    'ReadCapacityUnits': 10,
            #    'WriteCapacityUnits': 10
            #}
        )
    except:
        # table = dynamodb.Table(table_name)
        # status = table.table_status
        raise
        
    return table

def get_table(name, settings_cb):
    table = tables.get(name)
    if table:
        return table
    
    table = _get_table(name, settings_cb)
    tables[name] = table

    return table


