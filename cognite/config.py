config_api_key = ''
config_project = ''

def configure_session(api_key='', project=''):
    global config_api_key, config_project
    config_api_key = api_key
    config_project = project

def get_config_variables(api_key, project):
    if api_key == None:
        api_key = config_api_key
    if project == None:
        project = config_project
    return api_key, project