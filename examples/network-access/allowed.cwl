{
  "cwlVersion": "v1.2",
  "class": "CommandLineTool",
  "requirements": {
    "NetworkAccess": {
      "networkAccess": true
    },
    "ResourceRequirement": {
      "coresMin": 0.1,
      "ramMin": 128
    }
  },
  "baseCommand": [
    "python",
    "-c"
  ],
  "arguments": [
    "import json, sys, time, urllib.request, urllib.error\nurl = sys.argv[1]\ntry:\n    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))\n    with opener.open(url, timeout=5) as response:\n        result = {\"connected\": True, \"status\": response.status, \"url\": url}\nexcept urllib.error.HTTPError as error:\n    result = {\"connected\": True, \"status\": error.code, \"url\": url}\nexcept (urllib.error.URLError, TimeoutError, ConnectionError) as error:\n    result = {\"connected\": False, \"error\": str(error), \"url\": url}\nprint(json.dumps(result), flush=True)\ntime.sleep(3)\nsys.exit(0 if result[\"connected\"] else 1)\n"
  ],
  "inputs": {
    "url": {
      "type": "string",
      "inputBinding": {
        "position": 1
      }
    }
  },
  "stdout": "result.json",
  "outputs": {
    "result": {
      "type": "File",
      "outputBinding": {
        "glob": "result.json"
      }
    }
  }
}
