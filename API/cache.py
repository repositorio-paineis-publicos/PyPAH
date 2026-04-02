from functools import lru_cache
import hashlib
import json

_cache = {}

def make_key(nome: str, params: dict) -> str:
    raw = json.dumps({"nome": nome, "params": params}, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()

def get_cached(key: str):
    return _cache.get(key)

def set_cached(key: str, value):
    _cache[key] = value