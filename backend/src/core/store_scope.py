
import os
def current_store_id(): return os.getenv("STORE_ID","unknown_store")
def current_store_name(): return os.getenv("STORE_NAME","Unknown Store")
