import json
from logging import Logger
from typing import List, Dict
from datetime import datetime
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._logger = logger
        self._batch_size = 100

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")
        
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg['payload']
            self._stg_repository.order_events_insert(
                msg["object_id"],
                msg["object_type"],
                msg["sent_dttm"],
                json.dumps(order))
            
            self._logger.info(f"{datetime.utcnow()}: Message save db")

            user_id = order.get("user", {}).get("id")
            user = self._redis.get(user_id) if user_id else {}

            user_name = user.get("name", "unknown")

            # Получаем restaurant из order и Redis безопасно
            restaurant_id = order.get("restaurant", {}).get("id")
            restaurant = self._redis.get(restaurant_id) if restaurant_id else {}
            restaurant_name = restaurant.get("name", "unknown")  # если нет ключа, будет 'unknown'


            dst_msg = {
                "object_id": msg["object_id"],
                "object_type": "order",
                "payload": {
                    "id": msg["object_id"],
                    "date": order["date"],
                    "cost": order["cost"],
                    "payment": order["payment"],
                    "status": order["final_status"],
                    "restaurant": self._format_restaurant(restaurant_id, restaurant_name),
                    "user": self._format_user(user_id, user_name),
                    "products": self._format_items(order["order_items"], restaurant)
                }
            }

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")


        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_restaurant(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_user(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_items(self, order_items, restaurant) -> List[Dict[str, str]]:
        items = []

        # безопасно берём меню, если его нет — пустой список
        menu = restaurant.get("menu", [])

        for it in order_items:
            # безопасно ищем элемент в меню
            menu_item = next((x for x in menu if x.get("_id") == it.get("id")), {})
            dst_it = {
                "id": it.get("id"),
                "price": it.get("price"),
                "quantity": it.get("quantity"),
                "name": menu_item.get("name", "unknown"),
                "category": menu_item.get("category", "unknown")
            }
            items.append(dst_it)

        return items
