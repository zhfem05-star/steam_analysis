"""
API 응답 바로 확인하는 테스트 스크립트
"""


import requests
import json
from datetime import datetime

app_id = 730

url = f"https://store.steampowered.com/appreviews/{app_id}"

start_date = int(datetime(2000, 1, 1).timestamp())
end_date = int(datetime(2026, 9, 2).timestamp())

# date_range_type의 경우 넣지 않으면 날짜 필터가 안 됨(start_date, end_date 파라미터가 의미 없어짐)
params = {
                "json" : 1,
                "filter" : "recent",
                "purchase_type" : "all",
                # "review_type" : "all",
                "date_range_type": "include",
                "day_range": "all",
                "start_date" : start_date,
                "end_date" : end_date,
                "language": "all",
            }


resp = requests.get(url = url,params=params)


result = resp.json()

# temp_result = json.dumps(result["reviews"])
temp_result = json.dumps(result["query_summary"])

print(temp_result)
# print(result.keys())