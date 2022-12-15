import json
def readjson(file):
    f = open(file)
    data = json.load(f)
    f.close()
    return data