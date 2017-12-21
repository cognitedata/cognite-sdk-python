# GLOBAL CONSTANTS
_LIMIT = 10000

# GLOBAL VARIABLES
__config_api_key = ''
__config_project = ''

def configure_session(api_key='', project=''):
    global __config_api_key, __config_project
    __config_api_key = api_key
    __config_project = project

def _get_config_variables(api_key, project):
    if api_key == None:
        api_key = __config_api_key
    if project == None:
        project = __config_project
    return api_key, project