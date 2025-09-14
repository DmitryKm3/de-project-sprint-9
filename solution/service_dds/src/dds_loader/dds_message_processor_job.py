from datetime import datetime
from logging import Logger
import uuid

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger,
                 batch_size: int = 100) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            payload = msg.get("payload")
            if not payload:
                self._logger.warning(f"Skipping message without payload: {msg}")
                continue

            if payload.get("status") != "CLOSED":
                continue

            self._process_order(payload)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _process_order(self, payload: dict) -> None:
        load_dt = datetime.now()
        load_src = self._consumer.topic

        order_id = payload["id"]
        order_dt = payload["date"]
        order_status = payload["status"]
        order_cost = payload["cost"]
        order_payment = payload["payment"]

        user = payload["user"]
        restaurant = payload["restaurant"]

        # PK и хэши
        h_user_pk = self._uuid(user["id"])
        h_restaurant_pk = self._uuid(restaurant["id"])
        h_order_pk = self._uuid(str(order_id))

        hk_order_user_pk = self._uuid(h_order_pk + h_user_pk)
        hk_user_names_hashdiff = self._uuid(h_user_pk + user["name"])
        hk_restaurant_names_hashdiff = self._uuid(h_restaurant_pk + restaurant["name"])
        hk_order_cost_hashdiff = self._uuid(h_order_pk + str(order_cost) + str(order_payment))
        hk_order_status_hashdiff = self._uuid(h_order_pk + order_status)

        # Вставка в хабы
        self._dds_repository.h_user_insert(h_user_pk, user["id"], load_dt, load_src)
        self._dds_repository.h_restaurant_insert(h_restaurant_pk, restaurant["id"], load_dt, load_src)
        self._dds_repository.h_order_insert(h_order_pk, order_id, order_dt, load_dt, load_src)

        # Вставка в линк
        self._dds_repository.l_order_user_insert(hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)

        # Вставка в саттелиты
        self._dds_repository.s_user_names_insert(
            h_user_pk, user["name"], user["login"], load_dt, load_src, hk_user_names_hashdiff
        )
        self._dds_repository.s_restaurant_names_insert(
            h_restaurant_pk, restaurant["name"], load_dt, load_src, hk_restaurant_names_hashdiff
        )
        self._dds_repository.s_order_cost_insert(
            h_order_pk, order_cost, order_payment, load_dt, load_src, hk_order_cost_hashdiff
        )
        self._dds_repository.s_order_status_insert(
            h_order_pk, order_status, load_dt, load_src, hk_order_status_hashdiff
        )

        # обработка продуктов
        dst_msg = self._process_products(payload.get("products", []), h_order_pk, h_restaurant_pk, load_dt, load_src)

        # публикация в Kafka
        self._producer.produce(dst_msg)

    def _process_products(self, products: list, h_order_pk: str, h_restaurant_pk: str, load_dt, load_src) -> dict:
        product_ids, category_ids, product_names, category_names, order_cnts = [], [], [], [], []

        for product in products:
            h_product_pk = self._uuid(product["id"])
            h_category_pk = self._uuid(product["category"])

            hk_order_product_pk = self._uuid(h_order_pk + h_product_pk)
            hk_product_restaurant_pk = self._uuid(h_product_pk + h_restaurant_pk)
            hk_product_category_pk = self._uuid(h_product_pk + h_category_pk)
            hk_product_names_hashdiff = self._uuid(h_product_pk + product["name"])

            self._dds_repository.h_product_insert(h_product_pk, product["id"], load_dt, load_src)
            self._dds_repository.h_category_insert(h_category_pk, product["category"], load_dt, load_src)

            self._dds_repository.l_order_product_insert(hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
            self._dds_repository.l_product_restaurant_insert(hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
            self._dds_repository.l_product_category_insert(hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)

            self._dds_repository.s_product_names_insert(h_product_pk, product["name"], load_dt, load_src, hk_product_names_hashdiff)

            # формируем данные для output
            product_ids.append(h_product_pk)
            category_ids.append(h_category_pk)
            product_names.append(product["name"])
            category_names.append(product["category"])
            order_cnts.append(product.get("quantity", 1))

        return {
            "user_id": h_order_pk,         
            "product_id": product_ids,
            "product_name": product_names,
            "category_id": category_ids,
            "category_name": category_names,
            "order_cnt": order_cnts
        }
