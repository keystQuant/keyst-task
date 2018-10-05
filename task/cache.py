import redis, json
import pandas as pd


class RedisClient:

    def __init__(self):
        print('Connecting to cache server (Redis) on Gobble server')
        ip = '198.13.60.19'
        pw = 'da56038fa453c22d2c46e83179126e97d4d272d02ece83eb83a97357e842d065'
        self.redis_client = redis.Redis(host=ip,
                                        port=6379,
                                        password=pw)

    def set_key(self, key, value):
        response = self.redis_client.set(key, value)
        return response # returns True or False

    def get_key(self, key):
        response = self.redis_client.get(key)
        return response.decode("utf-8") # returns byte value without decode

    def key_exists(self, key):
        response = self.redis_client.exists(key)
        return response # returns True of False

    def del_key(self, key):
        response = self.redis_client.delete(key)
        return response # returns 1 or 0

    def set_list(self, data):
        response = self.redis_client.rpush(data[0], *data[1:])
        return response # returns 1 or 0

    def get_list(self, key, type='str'):
        response = self.redis_client.lrange(key, 0, -1)
        temp = response
        if type == 'int':
            try:
                is_int = int(response[0])
                response = list(map(lambda x: int(x), response))
            except ValueError:
                response = temp
        elif type == 'str':
            response = list(map(lambda x: x.decode('utf-8'), response))
        return response

    def add_to_list(self, key, data):
        response = self.redis_client.rpush(key, data)
        return response # returns 1 or 0

    def set_json(self, key, json):
        response = self.redis_client.set(key, json)
        return response # return True or False

    def get_json(self, key):
        response = json.loads(self.redis_client.get(key).decode('utf-8').replace('\'', '"'))
        return response

    def set_df(self, key, df):
        response = self.redis_client.set(key, df.to_msgpack(compress='zlib'))
        return response # returns True of False

    def get_df(self, key):
        response = pd.read_msgpack(self.redis_client.get(key))
        return response # returns DataFrame
