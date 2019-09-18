import codecs
import json
import pickle
import zlib

import pandas as pd

import redis


class RedisService:
  redis_client = None

  def __init__(self):
    port = 6379
    hostname = "localhost"
    password = ""

    self.redis_client = redis.StrictRedis(
      host=hostname,
      port=port,
      password=password,
      decode_responses=True,
      socket_timeout=100
    )

  def write_string(self, key: str, value: str):
    self.redis_client.set(key, value)

  def write_as_json(self, key: str, value: object):
    self.redis_client.set(key, json.dumps(value))

  def read(self, key):
    return self.redis_client.get(key)

  def read_as_json(self, key):
    result = self.redis_client.get(key)
    return None if result is None else json.loads(result)

  def write_df(self, key: str, df: pd.DataFrame):
    dumped_str = zlib.compress(pickle.dumps(df))
    encoded_str = codecs.encode(dumped_str, "base64").decode()
    self.redis_client.set(key, encoded_str)

  def read_df(self, key):
    redis_str = self.redis_client.get(key)
    encoded_str = codecs.decode(redis_str.encode(), "base64")
    pickled = zlib.decompress(encoded_str)
    return pickle.loads(pickled)

  def close_client_connection(self):
    self.redis_client.close()
