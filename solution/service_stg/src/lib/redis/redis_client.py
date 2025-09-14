import json
from typing import Dict, List, Union
import redis


class RedisClient:
    def __init__(self, host: str, port: int, password: str, cert_path: str) -> None:
        self._client = redis.StrictRedis(
            host=host,
            port=port,
            password=password,
            ssl=True,
            ssl_ca_certs=cert_path
        )

    def set(self, k: str, v: Union[Dict, List, str, int, float]) -> None:
        """Сохраняет объект в Redis в виде JSON строки"""
        self._client.set(k, json.dumps(v))

    def get(self, k: str) -> Dict:
        """Возвращает словарь по ключу. Если ключа нет, возвращает пустой словарь"""
        obj = self._client.get(k)
        if obj is None:
            # ключа нет в Redis
            return {}
        try:
            return json.loads(obj)
        except json.JSONDecodeError:
            # если по какой-то причине данные невалидный JSON
            return {}

    def mget(self, *keys: str) -> List[Dict]:
        """Возвращает список словарей по ключам. Пропавшие ключи заменяются пустым словарем"""
        values = self._client.mget(keys)
        result = []
        for v in values:
            if v is None:
                result.append({})
            else:
                try:
                    result.append(json.loads(v))
                except json.JSONDecodeError:
                    result.append({})
        return result
