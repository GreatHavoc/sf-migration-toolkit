import sys
import os

sys.path.insert(0, os.path.abspath("backend"))

try:
    from backend.app.api.connections import list_databases_route

    print("Import successful")
except Exception as e:
    import traceback

    traceback.print_exc()
