import re
import json

# Strips newlines from a string
# Replace single-quotes with backticks
def linearize(string: str) -> str:
    exp = re.compile(r"\s+")
    return exp.sub(" ", string).replace("'", "`").strip()

def camel_case(s):
  s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "")
  return ''.join([s[0].lower(), s[1:]])

# Merges two (potentially nested) dict structures, such as a default + custom config
def merge_dicts(user, default):
    if isinstance(user, dict) and isinstance(default, dict):
        for k, v in default.items():
            if k not in user:
                user[k] = v
            else:
                user[k] = merge_dicts(user[k], v)
    return user

# Converts (for example) `LocalEducationAgencies` to `LocalEducationAgency`; `students` to `student`; etc.
def singularize_endpoint(endpoint):
    if endpoint[-3:]=="ies": return endpoint[0:-3] + "y"
    elif endpoint=="people": return "person"
    else: return endpoint[0:-1]

# Takes a params structure and interpolates values from a (string) JSON payload
def interpolate_params(params_structure, payload):
    params = {}
    for k,v in params_structure.items():
        value = json.loads(payload)
        for key in v.split('.'):
            value = value[key]
        params[k] = value
    return params

def url_join(*args):
    return '/'.join(
        map(lambda x: str(x).rstrip('/'), filter(lambda x: x is not None, args))
    )