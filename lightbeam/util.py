import re
import json
import itertools
import copy

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
def pluralize_endpoint(endpoint):
    if endpoint[-1:]=="y": return endpoint[0:-1] + "ies"
    elif endpoint=="person": return "people"
    else: return endpoint+"s"

# Takes a params structure and interpolates values from a (string) JSON payload
def interpolate_params(params_structure, payload):
    params = {}
    for k,v in params_structure.items():
        value = payload.copy()
        for key in v.split('.'):
            value = value[key]
        params[k] = value
    return params

def url_join(*args):
    return '/'.join(
        map(lambda x: str(x).rstrip('/'), filter(lambda x: x is not None, args))
    )

def get_namespace_for_endpoint(endpoint, namespace, descriptor_namespace):
    """
    Returns the appropriate namespace for the given endpoint.
    Descriptor endpoints use descriptor_namespace, all others use namespace.
    """
    if endpoint.endswith("Descriptors"):
        return descriptor_namespace
    return namespace

# Returns the subset of `keys` that match the `keep` and `drop` criteria, importantly
# respecting wildcards! (so keep=["*Association,student*"] matches anything beginning
# with "student" or ending with "Association")
# This function is used for both the endpoint selection in apply_filters() of api.py and
# the keep-keys and drop-keys filtering in fetch.py
def apply_selections(keys, keep, drop):
    # `keep` and `drop` _should_ be arrays, but in case they're strings, we split them
    if isinstance(keep, str): keep = keep.split(",")
    if isinstance(drop, str): drop = drop.split(",")
    # this will be the filtered set of keys
    final_keys = []
    # populate `final_keys` with `keys` that match `keep`
    if keep and keep != ["*"]:
        for payload_key, keep_key in list(itertools.product(keys, keep)):
            if (keys_match(payload_key, keep_key)):
                final_keys.append(payload_key)
    # copy rather than direct pointer assignment, so this function doesn't modify the payload_keys variable in parent code
    else: final_keys = keys.copy()
    # remove from `final_keys` keys that match `drop`
    if drop and drop != [""]:
        for payload_key, drop_key in list(itertools.product(keys, drop)):
            if (keys_match(payload_key, drop_key)):
                if payload_key in final_keys: final_keys.remove(payload_key)
    return final_keys

# Compares a key like "stateAbbreviationDescriptors" with a (potentially wildcard) expression
# like "*Descriptors" for match.
def keys_match(key, wildcard_key):
    if key==wildcard_key: return True
    if wildcard_key.startswith("*") and key.endswith(wildcard_key.lstrip("*")): return True
    if wildcard_key.endswith("*") and key.startswith(wildcard_key.rstrip("*")): return True
    return False

def get_swagger_ref_for_endpoint(namespace, swagger, endpoint):
    if "definitions" in swagger.keys():
        return "#/definitions/" + camel_case(namespace) + "_" + singularize_endpoint(endpoint)
    elif "components" in swagger.keys() and "schemas" in swagger["components"].keys():
        return "#/components/schemas/" + camel_case(namespace) + "_" + singularize_endpoint(endpoint)

def resolve_swagger_ref(swagger, ref):
    if "definitions" in swagger.keys():
        definition = ref.replace("#/definitions/", "")
        return swagger["definitions"].get(definition, None)
    elif "components" in swagger.keys() and "schemas" in swagger["components"].keys():
        definition = ref.replace("#/components/schemas/", "")
        return swagger["components"]["schemas"].get(definition, None)
