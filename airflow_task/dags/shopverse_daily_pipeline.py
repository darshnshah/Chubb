from __future__ import annotations

from datetime import timedelta
import json
import os

import pendulum

from airflow.sdk import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from airflow.sensors.filesystem import FileSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

DEFAULT_ARGS = {
    "owner": "data_eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["alerts@shopverse.local"],  # change if you like
    "email_on_failure": True,
}


@dag(
    dag_id="shopverse_daily_pipeline",
    description="Daily batch pipeline for ShopVerse e-commerce data",
    schedule="0 1 * * *",  # 01:00 every day, for previous day's data
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["shopverse", "dwh"],
)
def shopverse_daily_pipeline():
    """
    Daily batch ETL:
    - Waits for customers/products/orders files
    - Loads into staging
    - Builds dim_customers, dim_products, fact_orders
    - Runs data quality checks
    - Branches on low-volume order days
    """

    min_order_threshold = int(
        Variable.get("shopverse_min_order_threshold", default_var="10")
    )

    # ------------------------------------------------------------------
    # 1. SENSORS: Wait for all three files for the logical date ({{ ds }})
    # ------------------------------------------------------------------
    with TaskGroup(group_id="wait_for_files") as wait_for_files:
        FileSensor(
            task_id="wait_for_customers",
            fs_conn_id="fs_default",
            filepath="{{ var.value.shopverse_data_base_path }}/landing/customers/customers_{{ ds_nodash }}.csv",
            poke_interval=60,
            timeout=60 * 60,
            mode="poke",
        )

        FileSensor(
            task_id="wait_for_products",
            fs_conn_id="fs_default",
            filepath="{{ var.value.shopverse_data_base_path }}/landing/products/products_{{ ds_nodash }}.csv",
            poke_interval=60,
            timeout=60 * 60,
            mode="poke",
        )

        FileSensor(
            task_id="wait_for_orders",
            fs_conn_id="fs_default",
            filepath="{{ var.value.shopverse_data_base_path }}/landing/orders/orders_{{ ds_nodash }}.json",
            poke_interval=60,
            timeout=60 * 60,
            mode="poke",
        )

    # ------------------------------------------------------------------
    # 2. STAGING TaskGroup: truncate + load
    #    (TaskFlow @task used here, returns row counts via XCom)
    # ------------------------------------------------------------------
    with TaskGroup(group_id="staging") as staging:
        truncate_stg_customers = SQLExecuteQueryOperator(
            task_id="truncate_stg_customers",
            conn_id="postgres_dwh",
            sql="TRUNCATE TABLE stg_customers;",
        )

        truncate_stg_products = SQLExecuteQueryOperator(
            task_id="truncate_stg_products",
            conn_id="postgres_dwh",
            sql="TRUNCATE TABLE stg_products;",
        )

        truncate_stg_orders = SQLExecuteQueryOperator(
            task_id="truncate_stg_orders",
            conn_id="postgres_dwh",
            sql="TRUNCATE TABLE stg_orders;",
        )

        @task(task_id="load_stg_customers")
        def load_stg_customers_task(customers_path: str) -> int:
            """Load customers CSV into stg_customers, return inserted row count."""
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            inserted = 0
            conn = hook.get_conn()
            conn.autocommit = False
            with conn, conn.cursor() as cur, open(customers_path, "r") as f:
                next(f)  # skip header
                for line in f:
                    parts = [p.strip() for p in line.strip().split(",")]
                    if len(parts) != 6:
                        continue
                    (
                        customer_id,
                        first_name,
                        last_name,
                        email,
                        signup_date,
                        country,
                    ) = parts
                    cur.execute(
                        """
                        INSERT INTO stg_customers
                        (customer_id, first_name, last_name, email, signup_date, country)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            customer_id,
                            first_name,
                            last_name,
                            email,
                            signup_date,
                            country,
                        ),
                    )
                    inserted += 1
            return inserted

        @task(task_id="load_stg_products")
        def load_stg_products_task(products_path: str) -> int:
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            inserted = 0
            conn = hook.get_conn()
            conn.autocommit = False
            with conn, conn.cursor() as cur, open(products_path, "r") as f:
                next(f)
                for line in f:
                    parts = [p.strip() for p in line.strip().split(",")]
                    if len(parts) != 4:
                        continue
                    product_id, product_name, category, unit_price = parts
                    cur.execute(
                        """
                        INSERT INTO stg_products
                        (product_id, product_name, category, unit_price)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (product_id, product_name, category, unit_price),
                    )
                    inserted += 1
            return inserted

        @task(task_id="load_stg_orders")
        def load_stg_orders_task(orders_path: str) -> int:
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            inserted = 0
            conn = hook.get_conn()
            conn.autocommit = False
            with conn, conn.cursor() as cur, open(orders_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    order = json.loads(line)
                    cur.execute(
                        """
                        INSERT INTO stg_orders
                        (order_id, order_timestamp, customer_id, product_id,
                         quantity, total_amount, currency, status)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            order.get("order_id"),
                            order.get("order_timestamp"),
                            order.get("customer_id"),
                            order.get("product_id"),
                            order.get("quantity"),
                            order.get("total_amount"),
                            order.get("currency"),
                            order.get("status"),
                        ),
                    )
                    inserted += 1
            return inserted

        customers_loaded = load_stg_customers_task(
            customers_path="{{ var.value.shopverse_data_base_path }}/landing/customers/customers_{{ ds_nodash }}.csv"
        )
        products_loaded = load_stg_products_task(
            products_path="{{ var.value.shopverse_data_base_path }}/landing/products/products_{{ ds_nodash }}.csv"
        )
        orders_loaded = load_stg_orders_task(
            orders_path="{{ var.value.shopverse_data_base_path }}/landing/orders/orders_{{ ds_nodash }}.json"
        )

        truncate_stg_customers >> customers_loaded
        truncate_stg_products >> products_loaded
        truncate_stg_orders >> orders_loaded

    # ------------------------------------------------------------------
    # 3. WAREHOUSE TaskGroup: build dims + fact_orders
    # ------------------------------------------------------------------
    with TaskGroup(group_id="warehouse") as warehouse:
        build_dim_customers = SQLExecuteQueryOperator(
            task_id="build_dim_customers",
            conn_id="postgres_dwh",
            sql="""
            INSERT INTO dim_customers (customer_id, first_name, last_name, email, signup_date, country)
            SELECT DISTINCT
                customer_id,
                first_name,
                last_name,
                email,
                signup_date::date,
                country
            FROM stg_customers sc
            ON CONFLICT (customer_id) DO UPDATE
              SET first_name = EXCLUDED.first_name,
                  last_name  = EXCLUDED.last_name,
                  email      = EXCLUDED.email,
                  signup_date = EXCLUDED.signup_date,
                  country    = EXCLUDED.country;
            """,
        )

        build_dim_products = SQLExecuteQueryOperator(
            task_id="build_dim_products",
            conn_id="postgres_dwh",
            sql="""
            INSERT INTO dim_products (product_id, product_name, category, unit_price)
            SELECT DISTINCT
                product_id,
                product_name,
                category,
                unit_price::numeric
            FROM stg_products sp
            ON CONFLICT (product_id) DO UPDATE
              SET product_name = EXCLUDED.product_name,
                  category     = EXCLUDED.category,
                  unit_price   = EXCLUDED.unit_price;
            """,
        )

        build_fact_orders = SQLExecuteQueryOperator(
            task_id="build_fact_orders",
            conn_id="postgres_dwh",
            sql="""
            -- Clear today's slice before reloading
            DELETE FROM fact_orders
            WHERE order_date = {{ ds }};

            INSERT INTO fact_orders
            (
                order_id,
                order_timestamp_utc,
                order_date,
                customer_id,
                product_id,
                quantity,
                total_amount_usd,
                currency,
                currency_mismatch_flag,
                status
            )
            SELECT
                o.order_id,
                (o.order_timestamp AT TIME ZONE 'UTC') AS order_timestamp_utc,
                DATE(o.order_timestamp) AS order_date,
                o.customer_id,
                o.product_id,
                o.quantity,
                CASE WHEN o.currency = 'USD'
                     THEN o.total_amount::numeric
                     ELSE NULL
                END AS total_amount_usd,
                o.currency,
                CASE WHEN o.currency <> 'USD' THEN TRUE ELSE FALSE END AS currency_mismatch_flag,
                o.status
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_id
                        ORDER BY order_timestamp DESC
                    ) AS rn
                FROM stg_orders
            ) o
            WHERE
                rn = 1
                AND quantity > 0
                AND customer_id IS NOT NULL
                AND product_id IS NOT NULL;
            """,
        )

        [build_dim_customers, build_dim_products] >> build_fact_orders

    # ------------------------------------------------------------------
    # 4. DATA QUALITY CHECKS (TaskFlow)
    # ------------------------------------------------------------------
    @task(task_id="dq_check_dim_customers_not_empty")
    def dq_check_dim_customers_not_empty():
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        records = hook.get_first("SELECT COUNT(*) FROM dim_customers;")
        if not records or records[0] <= 0:
            raise ValueError("DQ failed: dim_customers is empty")
        return records[0]

    @task(task_id="dq_check_fact_orders_not_null_keys")
    def dq_check_fact_orders_not_null_keys(ds: str):
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        sql = """
            SELECT COUNT(*) FROM fact_orders
            WHERE order_date = %s
              AND (customer_id IS NULL OR product_id IS NULL);
        """
        bad_count = hook.get_first(sql, parameters=(ds,))[0]
        if bad_count > 0:
            raise ValueError(
                f"DQ failed: {bad_count} rows in fact_orders have NULL customer_id/product_id"
            )
        return True

    @task(task_id="dq_check_fact_orders_rowcount_match")
    def dq_check_fact_orders_rowcount_match(ds: str):
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        valid_stg_sql = """
            SELECT COUNT(*) FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_id
                        ORDER BY order_timestamp DESC
                    ) AS rn
                FROM stg_orders
            ) o
            WHERE
                rn = 1
                AND quantity > 0
                AND customer_id IS NOT NULL
                AND product_id IS NOT NULL
                AND DATE(order_timestamp) = %s;
        """
        (valid_stg_count,) = hook.get_first(valid_stg_sql, parameters=(ds,))
        (fact_count,) = hook.get_first(
            "SELECT COUNT(*) FROM fact_orders WHERE order_date = %s;",
            parameters=(ds,),
        )
        if fact_count != valid_stg_count:
            raise ValueError(
                f"DQ failed: fact_orders rows ({fact_count}) != valid staging orders ({valid_stg_count})"
            )
        return {"valid_stg_count": valid_stg_count, "fact_count": fact_count}

    dq_dim_customers = dq_check_dim_customers_not_empty()
    dq_fact_not_null_keys = dq_check_fact_orders_not_null_keys(ds="{{ ds }}")
    dq_fact_rowcount_match = dq_check_fact_orders_rowcount_match(ds="{{ ds }}")

    # ------------------------------------------------------------------
    # 5. BRANCHING ON VOLUME + anomaly summary
    # ------------------------------------------------------------------
    def _branch_on_volume(**context):
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        (order_count,) = hook.get_first(
            "SELECT COUNT(*) FROM fact_orders WHERE order_date = %s;",
            parameters=(context["ds"],),
        )
        if order_count < min_order_threshold:
            return "warn_low_volume"
        return "normal_completion"

    branch = BranchPythonOperator(
        task_id="branch_on_order_volume",
        python_callable=_branch_on_volume,
    )

    @task(task_id="warn_low_volume")
    def warn_low_volume_task(ds: str):
        """
        Low-volume branch:
        - log a warning
        - write a JSON summary file under data/anomalies
        """
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        (order_count,) = hook.get_first(
            "SELECT COUNT(*) FROM fact_orders WHERE order_date = %s;",
            parameters=(ds,),
        )
        base_path = Variable.get("shopverse_data_base_path")
        anomalies_dir = os.path.join(base_path, "anomalies")
        os.makedirs(anomalies_dir, exist_ok=True)
        path = os.path.join(anomalies_dir, f"low_volume_orders_{ds}.json")
        summary = {
            "order_date": ds,
            "order_count": order_count,
            "threshold": min_order_threshold,
            "message": "Low order volume detected in fact_orders",
        }
        with open(path, "w") as f:
            f.write(json.dumps(summary, indent=2))
        print(
            f"[WARN] Low volume for {ds}: {order_count} orders (threshold={min_order_threshold}). "
            f"Summary at {path}"
        )
        return path

    warn_low_volume = warn_low_volume_task(ds="{{ ds }}")
    normal_completion = EmptyOperator(task_id="normal_completion")

    # ------------------------------------------------------------------
    # 6. START / END + wiring
    # ------------------------------------------------------------------
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> wait_for_files >> staging >> warehouse
    warehouse >> [dq_dim_customers, dq_fact_not_null_keys, dq_fact_rowcount_match]
    [dq_dim_customers, dq_fact_not_null_keys, dq_fact_rowcount_match] >> branch
    branch >> [warn_low_volume, normal_completion] >> end


dag_instance = shopverse_daily_pipeline()
