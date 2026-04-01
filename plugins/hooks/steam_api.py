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
        cursors: dict[str, str] | None = None,
        num_reviews: int | None = None,
        languages: list[str] | None = None,
        filter_type: str = "recent",
    ) -> tuple[list[dict], dict[str, str]]:
        """
        게임 리뷰 조회 (다국어 + cursor 기반 증분 수집)

        :param cursors:     언어별 시작 cursor. {"korean": "AoJ...", "english": "AoJ..."}
                            None 또는 키 누락 시 해당 언어를 처음부터("*") 수집.
        :param num_reviews: 언어별 최대 수집 수. None이면 전체 수집.
        :param languages:   수집할 언어 목록. 기본값 ["korean", "english"]
        :return: (중복 제거된 리뷰 리스트, 언어별 마지막 cursor 딕셔너리)
                 next_cursors는 tracked_games.review_cursors에 저장하여 다음 실행 시 재사용.
        """
        if languages is None:
            languages = ["korean", "english"]
        if cursors is None:
            cursors = {}

        seen_ids: set[int] = set()
        all_reviews: list[dict] = []
        next_cursors: dict[str, str] = {}

        for language in languages:
            start_cursor = cursors.get(language, "*")
            lang_reviews, last_cursor = self._get_reviews_for_language(
                appid=appid,
                language=language,
                start_cursor=start_cursor,
                num_reviews=num_reviews,
                filter_type=filter_type,
            )
            next_cursors[language] = last_cursor
            for review in lang_reviews:
                rid = review.get("recommendationid")
                if rid not in seen_ids:
                    seen_ids.add(rid)
                    all_reviews.append(review)
            log.info(
                "appid=%s language=%s 수집 완료: %d개 (전체 누적 %d개)",
                appid, language, len(lang_reviews), len(all_reviews),
            )

        return all_reviews, next_cursors

    def _get_reviews_for_language(
        self,
        appid: int,
        language: str,
        start_cursor: str = "*",
        num_reviews: int | None = None,
        filter_type: str = "recent",
    ) -> tuple[list[dict], str]:
        """
        단일 언어 리뷰 페이지네이션.

        :return: (리뷰 리스트, 마지막으로 받은 cursor)
                 cursor는 다음 실행의 start_cursor로 사용하여 이어서 수집 가능.
        """
        url = f"{_BASE_URL_STORE}/appreviews/{appid}"
        reviews: list[dict] = []
        cursor = start_cursor
        last_cursor = start_cursor

        while True:
            remaining = None if num_reviews is None else num_reviews - len(reviews)
            if remaining is not None and remaining <= 0:
                break

            params = {
                "json": 1,
                "filter": filter_type,
                "language": language,
                "num_per_page": 100 if remaining is None else min(100, remaining),
                "cursor": cursor,
                "purchase_type": "all",
            }

            try:
                data = self._request(url, params)
            except Exception as e:
                # 네트워크/타임아웃 오류 — 마지막으로 성공한 cursor까지만 반환
                log.error(
                    "페이지 요청 실패, 수집 중단: appid=%s language=%s cursor=%s error=%s",
                    appid, language, cursor, e,
                )
                break

            if data.get("success") != 1:
                log.warning("리뷰 조회 실패: appid=%s language=%s", appid, language)
                break

            batch = data.get("reviews", [])
            if not batch:
                break

            reviews.extend(batch)
            last_cursor = data.get("cursor", cursor)
            cursor = last_cursor
            log.info("리뷰 %d개 수신 (누적: %d, 언어=%s)", len(batch), len(reviews), language)
            self._wait()

        return reviews, last_cursor

    def iter_review_pages(
        self,
        appid: int,
        language: str,
        start_cursor: str = "*",
        filter_type: str = "recent",
    ):
        """
        단일 언어 리뷰를 페이지 단위로 yield하는 제너레이터.

        rate limit 대기(sleep)를 yield 이후에 수행하므로,
        호출 측에서 yield된 데이터를 처리하는 동안 대기 시간이 겹친다.

        :yield: (page_reviews: list[dict], next_cursor: str)
        """
        url = f"{_BASE_URL_STORE}/appreviews/{appid}"
        cursor = start_cursor

        while True:
            params = {
                "json": 1,
                "filter": filter_type,
                "language": language,
                "num_per_page": 100,
                "cursor": cursor,
                "purchase_type": "all",
            }

            try:
                data = self._request(url, params)
            except Exception as e:
                log.error(
                    "페이지 요청 실패, 중단: appid=%s language=%s cursor=%s error=%s",
                    appid, language, cursor, e,
                )
                return

            if data.get("success") != 1:
                log.warning("리뷰 조회 실패: appid=%s language=%s", appid, language)
                return

            batch = data.get("reviews", [])
            if not batch:
                return

            next_cursor = data.get("cursor", cursor)
            log.info("페이지 수신: appid=%s language=%s 리뷰=%d개", appid, language, len(batch))

            yield batch, next_cursor  # consumer가 이 데이터를 처리하는 동안

            cursor = next_cursor
            self._wait()              # rate limit 대기 — consumer 처리와 시간이 겹침

    def get_query(
            self,
            query_name: str = "",
            start: int = 0,
            count: int = 1000,
            sort: int = 0,
            filters: dict | None = None,
            context: dict | None = None,
            data_request: dict | None = None,
    ) -> dict:
        """
        IStoreQueryService/Query/v1 호출

        :params:
            query_name: 로깅용 쿼리 이름 (결과에 영향 없음)
            start: 페이지네이션 시작 위치
            count: 반환할 항목 수 (최대 1000)
            sort: 정렬 기준
                RELEVANCE = 0       # 기본 정렬 (관련성)
                NAME_ASC = 1        # 이름순 (가나다/ABC)
                RELEASE_ASC = 2     # 출시일 오래된 순
                POPULARITY = 10     # 인기순
                TRENDING = 11       # 트렌딩
                TOP_SELLERS = 12    # 베스트셀러
                RECENT_POPULAR = 13 # 최근 인기
            filters: query.filters 객체
                released_only           bool    출시된 게임만 포함
                type_filters:
                    include_games       bool    게임 포함
                    include_dlc         bool    DLC 포함
                price_filters:
                    exclude_free_items      bool    무료 게임 제외
                    min_discount_percent    int32   최소 할인율 (1이상 → 할인 중인 게임만)
            context: 언어/국가 설정 {"language": "korean", "country_code": "KR"}
            data_request: 응답에 포함할 데이터 (미설정 시 appid만 반환)
                include_basic_info              bool    이름, 설명 등 기본 정보
                include_all_purchase_options    bool    가격, 할인 정보
        """
        url = f"{_BASE_URL_API}/IStoreQueryService/Query/v1/"

        # start, count, sort, filters는 query 객체 안에 중첩해야 함
        input_data = {
            "query_name": query_name,
            "query": {
                "start": start,
                "count": count,
                "sort": sort,
                "filters": filters or {},
            },
            "context": context or {},
            "data_request": data_request or {},
        }

        params = {
            "key": self.api_key,
            "input_json": json.dumps(input_data),
        }

        data = self._request(url, params)
        return data
