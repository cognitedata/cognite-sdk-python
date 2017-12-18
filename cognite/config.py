API_KEY = ''
PROJECT = ''

def configure_session(api_key='', project=''):
    global API_KEY, PROJECT
    API_KEY = api_key
    PROJECT = project

def get_config_variables(api_key, project):
    if api_key == None:
        api_key = API_KEY
    if project == None:
        project = PROJECT
    return api_key, project