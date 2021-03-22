import json
import boto3

from sip_data import SIPDataFull, SIPDataInc
from sip_bucket import SIPBucketFull, SIPBucketInc
from sip_trim import SIPMarkerHandler

from env import *


class SIPEnv:
    def __init__(self, event):
        self.event = event
        self.params = event['queryStringParameters'] or {}

    def list_providers(self):
        return [ 'data.full', 'data.inc', 'bucket.full', 'bucket.inc' ]

    def find_provider(self, provider, instance):
        if provider == 'data.full':
            return SIPDataFull(self)

        if provider == 'data.inc':
            return SIPDataInc(self)

        if provider == 'bucket.full':
            return SIPBucketFull(self, instance)

        if provider == 'bucket.inc':
            return SIPBucketInc(self, instance)

        return None



class SIPGetInfo:
    def __init__(self, env, provider):
        self.env = env
        self.provider = provider

    def exec(self):
        params = self.env.params
        opt_instance = params.get('instance')

        pvd = self.env.find_provider(self.provider, opt_instance)
        if not pvd:
            return (404, {})

        return pvd.info()

class SIPGetStatus:
    def __init__(self, env, provider):
        self.env = env
        self.provider = provider

    def exec(self):
        params = self.env.params
        opt_instance = params.get('instance')
        opt_stage_id = params.get('stage-id')
        shard_id = int(params.get('shard-id', '0'))

        pvd = self.env.find_provider(self.provider, opt_instance)
        if not pvd:
            return (404, {})

        return pvd.status(opt_stage_id, shard_id)

class SIPFetch:
    def __init__(self, env, provider):
        self.env = env
        self.provider = provider

    def exec(self):
        params = self.env.params
        opt_instance = params.get('instance')
        opt_stage_id = params.get('stage-id')
        marker = params.get('marker', '')
        max_entries = int(params.get('max', '1000'))
        shard_id = int(params.get('shard-id', '0'))

        pvd = self.env.find_provider(self.provider, opt_instance)
        if not pvd:
            return (404, {})

        return pvd.fetch(opt_stage_id, shard_id, marker, max_entries)

class SIPSetMarkerInfo:
    def __init__(self, env, provider, body):
        self.env = env
        self.provider = provider
        self.body = body

    def exec(self):
        params = self.env.params
        opt_instance = params.get('instance')
        opt_stage_id = params.get('stage-id')
        shard_id = int(params.get('shard-id', '0'))

        try:
            body_dict = json.loads(self.body)

            target_id = body_dict['target_id']
            marker = body_dict['marker']
            mtime = body_dict['mtime']
            check_exists = bool(body_dict['check_exists'])
        except:
            return (400, {})

        handler = SIPMarkerHandler(self.env,
                                   self.provider,
                                   opt_instance,
                                   opt_stage_id,
                                   shard_id)

        if not handler.set_target_info(target_id,
                                       marker,
                                       mtime,
                                       check_exists):
            return (400, {})
        
        min_marker = handler.read_min_marker()
        
        logger.info('after update: target_id=%s marker=%s, min_marker=%s'  % (target_id, marker, min_marker))
        
        if min_marker < marker:
            # all done, min marker is smaller, can't trim
            return (200, {})
        
        pvd = self.env.find_provider(self.provider, opt_instance)
        if not pvd:
             return (404, {})
        
        return pvd.trim(opt_stage_id, shard_id, min_marker)
        
        


class HttpGet:
    def __init__(self, env):
        self.env = env

    def exec(self):
        params = self.env.params

        opt_provider = params.get('provider')
        opt_data_type = params.get('data-type')
        opt_stage_type = params.get('stage-type')
        opt_info = params.get('info')
        opt_status = params.get('status')

        if not opt_provider and (opt_data_type and opt_stage_type):
            opt_provider = opt_data_type + '.' + opt_stage_type

        if not opt_provider:
            return (200, self.env.list_providers())

        if opt_info is not None:
            op = SIPGetInfo(self.env, opt_provider)
        elif opt_status is not None:
            op = SIPGetStatus(self.env, opt_provider)
        else:
            op = SIPFetch(self.env, opt_provider)

        return op.exec()


class HttpPut:
    def __init__(self, event, env):
        self.event = event
        self.env = env

    def exec(self):
        params = self.env.params

        opt_provider = params.get('provider')
        opt_marker_info = params.get('marker-info')

        body = self.event['body']

        if not opt_provider or not body:
            return (405, {})

        op = None

        if opt_marker_info is not None:
            op = SIPSetMarkerInfo(self.env, opt_provider, body)

        if not op:
            return (405, {})

        return op.exec()
    

def lambda_handler(event, context):

    logger.info('### event:')
    logger.info(json.dumps(event))

    if not env_params.valid():
        return {
        'statusCode': 500,
        'body': json.dumps({'status': 'misconfigured'})
        }

    method = event['httpMethod']

    env = SIPEnv(event)
    
    result = {}
    
    if method == 'GET':
        handler = HttpGet(env)
    elif method == 'PUT':
        handler = HttpPut(event, env)
    else:
        handler = None
    
    if handler:
        status, body = handler.exec()
    else:
        status = 405
        body = {}
        
    
    
    return {
        'statusCode': status,
        'body': json.dumps(body)
    }

