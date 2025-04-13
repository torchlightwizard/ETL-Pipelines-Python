import importlib
import os



def get (path="./dashboard/dashboards"):
    html = []
    files = [file for file in os.listdir(path) if (os.path.isfile(os.path.join(path, file)) and file != "__init__.py")]
    files = [file.split(".")[0] for file in files]
    for file in files:
        module = importlib.import_module(f"dashboard.dashboards.{file}")
        element = getattr(module, "element")
        html.append(element)
    return html