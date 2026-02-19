"""
Custom Airflow Plugins

airflow DAG에 쓰이는 커스텀 플러그인
"""

from airflow.plugins_manager import AirflowPlugin

from hooks.s3_hook import SteamS3Hook
from hooks.steam_api import SteamApiHook
from operators.steam_api_to_s3 import SteamApiToS3Operator


class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [SteamApiToS3Operator]
    hooks = [SteamS3Hook, SteamApiHook]