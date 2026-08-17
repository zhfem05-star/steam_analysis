from airflow.hooks.base import BaseHook
from pyspark.sql import SparkSession


class SteamSparkHook:
    """Spark 클러스터 연결을 위한 프로젝트 전용 Hook"""

    def __init__(self, spark_conn_id: str = "spark_default", app_name: str = "steam-project"):
        self.spark_conn_id = spark_conn_id
        self.app_name = app_name

    def get_session(self, packages: list[str] | None = None) -> SparkSession:
        """
        :param packages: 추가로 필요한 Maven 좌표 목록 (예: Kafka 커넥터).
                          job마다 필요한 게 다르므로 Hook에 고정하지 않고 호출 시점에 지정.
        """
        conn = BaseHook.get_connection(self.spark_conn_id)
        master_url = f"spark://{conn.host}:{conn.port}"
        builder = SparkSession.builder.master(master_url).appName(self.app_name)
        if packages:
            builder = builder.config("spark.jars.packages", ",".join(packages))
        return builder.getOrCreate()