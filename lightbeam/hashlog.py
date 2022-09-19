import os
import pickle
import hashlib


# Loads (unpickles) a hashlog file
def load(file):
    hashlog = {}
    if os.path.isfile(file):
        with open(file, 'rb') as f:
            hashlog = pickle.load(f)
    # else:
    #     raise Exception(f"hashlog file {file} does not exist")
    return hashlog

# Saves (pickles) a hashlog file
def save(file, data):
    state_dir = os.path.dirname(file)
    if not os.path.isdir(state_dir):
        os.mkdir(state_dir)
    with open(file, 'wb') as f:
        pickle.dump(data, f)

def get_hash(data):
    return hashlib.md5(data.encode()).digest()

def get_hash_string(data):
    return hashlib.md5(data.encode()).hexdigest()