AMQP_URI_CONFIG_KEY = 'AMQP_URI'
WEB_SERVER_CONFIG_KEY = 'WEB_SERVER_ADDRESS'
WEB_SERVER_ALLOW_CORS_CONFIG_KEY = 'WEB_SERVER_ALLOW_CORS'
WEB_SERVER_CORS_HEADERS_CONFIG_KEY = 'WEB_SERVER_CORS_HEADERS'
RPC_EXCHANGE_CONFIG_KEY = 'rpc_exchange'
SERIALIZER_CONFIG_KEY = 'serializer'
HEARTBEAT_CONFIG_KEY = 'HEARTBEAT'

MAX_WORKERS_CONFIG_KEY = 'max_workers'
PARENT_CALLS_CONFIG_KEY = 'parent_calls_tracked'

DEFAULT_WEB_SERVER_CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': ('Content-Type, Authorization'),
    'Access-Control-Allow-Methods': ('HEAD, POST, GET, PUT, DELETE, OPTIONS'),
}
DEFAULT_MAX_WORKERS = 10
DEFAULT_PARENT_CALLS_TRACKED = 10
DEFAULT_SERIALIZER = 'json'
DEFAULT_RETRY_POLICY = {'max_retries': 3}
DEFAULT_HEARTBEAT = 60

CALL_ID_STACK_CONTEXT_KEY = 'call_id_stack'
AUTH_TOKEN_CONTEXT_KEY = 'auth_token'
LANGUAGE_CONTEXT_KEY = 'language'
USER_ID_CONTEXT_KEY = 'user_id'
USER_AGENT_CONTEXT_KEY = 'user_agent'
