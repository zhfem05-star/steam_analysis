"""
steam 프로젝트 전용 Snowflake hook
SF의 커넥션 객체로 추후 DW로 이관작업을 하기 위해
미리 hook을 작성
"""
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class Steam_SF_Hook:
    """
    SF 데이터 파이프라인 커넥션 연결용 Hook
    """
    def __init__(self, snowflake_conn_id:str = "snowflake_default"):
        self._hook = SnowflakeHook(snowflake_conn_id = snowflake_conn_id)