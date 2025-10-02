from data_access_service.server import api_setup, app

# this one is a global api instance for batch running
batch_api = api_setup(app)
