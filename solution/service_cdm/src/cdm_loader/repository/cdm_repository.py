from typing import Any, Dict
from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_product_counters_insert(
        self, user_id: str, product_id: str, product_name: str, order_cnt: int
    ) -> None:
        
        query = """
            INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
            VALUES (%(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s)
            ON CONFLICT (user_id, product_id) DO UPDATE
            SET order_cnt = user_product_counters.order_cnt + EXCLUDED.order_cnt;
        """
        params = {
            "user_id": user_id,
            "product_id": product_id,
            "product_name": product_name,
            "order_cnt": order_cnt,
        }
        self._execute(query, params)

    def user_category_counters_insert(
        self, user_id: str, category_id: str, category_name: str
    ) -> None:
       
        query = """
            INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
            VALUES (%(user_id)s, %(category_id)s, %(category_name)s, 1)
            ON CONFLICT (user_id, category_id) DO UPDATE
            SET order_cnt = user_category_counters.order_cnt + 1;
        """
        params = {
            "user_id": user_id,
            "category_id": category_id,
            "category_name": category_name,
        }
        self._execute(query, params)

    def _execute(self, query: str, params: Dict[str, Any]) -> None:
        """
        Вспомогательный метод для выполнения SQL-запроса.
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
