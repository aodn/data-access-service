import psutil

def get_memory_usage_percent():
    return psutil.virtual_memory().percent