"""
Steam API 통합 Hook

Steam Web API(공식) 및 Store API(비공식) 호출을 담당한다.
API key 관리, rate limit 대기, 페이지네이션을 한 곳에서 처리.
"""

from __future__ import annotations

import json
import logging
import time

import requests
from airflow.models import Variable

log = logging.getLogger(__name__)

# Steam API 기본 설정
_BASE_URL_API = "https://api.steampowered.com"
_BASE_URL_STORE = "https://store.steampowered.com"
_RATE_LIMIT_SEC = 1.5
_TIMEOUT = 30


class SteamApiHook:
    """
    Steam API 통합 Hook

    공식/비공식 API를 메서드별로 제공하며,
    rate limit·에러 처리·페이지네이션을 내부에서 처리한다.
    """

    def __init__(self, api_key: str | None = None):
        """
        :param api_key: Steam Web API Key.
                        None이면 Airflow Variable 'steam_api_key'에서 가져온다.
        """
        self._api_key = api_key

    @property
    def api_key(self) -> str:
        if self._api_key is None:
            self._api_key = Variable.get("steam_api_key")
        return self._api_key

    # ── 내부 공통 메서드 ─────────────────────────────

    def _request(self, url: str, params: dict | None = None) -> dict:
        """HTTP GET 요청 + 에러 로깅"""
        params = params or {}
        log.info("Steam API 요청: %s  params=%s", url, params)

        try:
            resp = requests.get(url, params=params, timeout=_TIMEOUT)
        except requests.ConnectionError:
            log.error("네트워크 연결 실패: %s", url)
            raise
        except requests.Timeout:
            log.error("요청 타임아웃(%ss): %s", _TIMEOUT, url)
            raise

        if not resp.ok:
            log.error(
                "API 응답 에러: status=%s  body=%s",
                resp.status_code,
                resp.text[:500],
            )
            resp.raise_for_status()

        return resp.json()

    def _wait(self, seconds: float = _RATE_LIMIT_SEC) -> None:
        """Rate limit 대기"""
        time.sleep(seconds)

    # ── 공식 API: IStoreService ───────────────────────

    def get_app_list(
        self,
        max_results: int = 50000,
        include_games: bool = True,
        include_dlc: bool = False,
        include_software: bool = False,
    ) -> list[dict]:
        """
        전체 Steam 앱 목록 조회 (페이지네이션 자동 처리)

        :return: [{"appid": 730, "name": "Counter-Strike 2", ...}, ...]
        """
        url = f"{_BASE_URL_API}/IStoreService/GetAppList/v1/"
        all_apps = []
        last_appid = 0

        while True:
            params = {
                "key": self.api_key,
                "max_results": max_results,
                "last_appid": last_appid,
                "include_games": include_games,
                "include_dlc": include_dlc,
                "include_software": include_software,
            }

            data = self._request(url, params)
            response = data.get("response", {})
            apps = response.get("apps", [])
            all_apps.extend(apps)

            log.info("앱 %d개 수신 (누적: %d)", len(apps), len(all_apps))

            if not response.get("have_more_results"):
                break

            last_appid = response["last_appid"]
            self._wait()

        return all_apps

    # ── 공식 API: ISteamUserStats ─────────────────────

    def get_current_players(self, appid: int) -> int | None:
        """
        특정 게임의 현재 동접자 수 조회 (API key 불필요)

        :return: 동접자 수. 실패 시 None
        """
        url = f"{_BASE_URL_API}/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
        data = self._request(url, {"appid": appid})
        response = data.get("response", {})

        if response.get("result") == 1:
            return response["player_count"]

        log.warning("동접자 조회 실패: appid=%s  response=%s", appid, response)
        return None

    # ── 비공식 API: Store ─────────────────────────────

    def get_app_details(
        self,
        appid: int,
        cc: str = "kr",
        language: str = "korean",
    ) -> dict | None:
        """
        게임 상세 정보 조회

        :return: 앱 상세 데이터. 실패 시 None
        """
        url = f"{_BASE_URL_STORE}/api/appdetails"
        data = self._request(url, {"appids": appid, "cc": cc, "l": language})

        app_data = data.get(str(appid), {})
        if app_data.get("success"):
            return app_data["data"]

        log.warning("앱 상세 조회 실패: appid=%s", appid)
        return None

    def get_app_reviews(
        self,
        appid: int,
        num_reviews: int = 100,
        language: str = "all",
        filter_type: str = "recent",
    ) -> list[dict]:
        """
        게임 리뷰 조회 (cursor 페이지네이션 자동 처리)

        :return: 리뷰 리스트
        """
        url = f"{_BASE_URL_STORE}/appreviews/{appid}"
        reviews = []
        cursor = "*"

        while len(reviews) < num_reviews:
            params = {
                "json": 1,
                "filter": filter_type,
                "language": language,
                "num_per_page": min(100, num_reviews - len(reviews)),
                "cursor": cursor,
                "purchase_type": "all",
            }

            data = self._request(url, params)

            if data.get("success") != 1:
                log.warning("리뷰 조회 실패: appid=%s", appid)
                break

            batch = data.get("reviews", [])
            if not batch:
                break

            reviews.extend(batch)
            cursor = data.get("cursor")

            log.info("리뷰 %d개 수신 (누적: %d)", len(batch), len(reviews))
            self._wait()

        return reviews[:num_reviews]



    def get_query(
            self,
            query_name: str, 
            query: str, 
            context: str = "", 
            data_request: str = "", 
            override_country_code: str = "",
            start: int = 0,
            count: int = 1000, # type: ignore
            sort: int = 0,
            filters: dict = None
    ) -> dict:
        """
        :params: 
            최상위 파라미터: key, query_name, query, context, data_request, override_country_code
            query: start, count, sort, filters
            sort 카테고리: 
                RELEVANCE = 0       # 기본 정렬 (관련성)
                NAME_ASC = 1        # 이름순 (가나다/ABC)
                RELEASE_ASC = 2     # 출시일 오래된 순
                POPULARITY = 10     # 인기순
                TRENDING = 11       # 트렌딩
                TOP_SELLERS = 12    # 베스트셀러
                RECENT_POPULAR = 13 # 최근 인기
            filters:
                released_only   bool    #출시된 게임만 포함
                coming_soon_only    bool    #출시 예정 게임만 포함
                type_filter:
                    include_apps	bool	#앱 포함
                    include_packages	bool	#패키지 포함
                    include_bundles	bool	#번들 포함
                    include_games	bool	#게임 포함
                    include_demos	bool	#데모 포함
                    include_mods	bool	#모드 포함
                    include_dlc	bool	#DLC 포함
                    include_software	bool	#소프트웨어 포함
                price_filters:
                    only_free_items     bool	무료 게임만
                    exclude_free_items  bool	무료 게임 제외
                    min_discount_percent	int32	최소 할인율 (%) — 1 이상 설정 시 할인 중인 게임만 반환
        """
        url = f"{_BASE_URL_API}/IStoreQueryService/Query/v1/"

        # protobuf 기반 API라 중첩 구조를 input_json으로 전달해야 함
        input_data = {
            "query_name": query_name,
            "query": query,
            "context": context,
            "data_request": data_request,
            "override_country_code": override_country_code,
            "start": start,
            "count": count,
            "sort": sort,
            "filters": filters or {},
        }

        params = {
            "key": self.api_key,
            "input_json": json.dumps(input_data),
        }

        data = self._request(url, params)
        return data
