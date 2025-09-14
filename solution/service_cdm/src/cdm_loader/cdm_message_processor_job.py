from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository


class CdmMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        cdm_repository: CdmRepository,
        logger: Logger,
        batch_size: int = 100
    ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: CDM processor started")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._process_message(msg)

        self._logger.info(f"{datetime.utcnow()}: CDM processor finished")

    def _process_message(self, msg: dict) -> None:
        """
        Обработка одного сообщения из Kafka.
        """
        user_id = msg["user_id"]
        products_info = zip(
            msg["product_id"],
            msg["product_name"],
            msg["category_id"],
            msg["category_name"],
            msg["order_cnt"],
        )

        for product_id, product_name, category_id, category_name, order_cnt in products_info:
            self._cdm_repository.user_product_counters_insert(
                user_id, product_id, product_name, order_cnt
            )
            self._cdm_repository.user_category_counters_insert(
                user_id, category_id, category_name
            )
