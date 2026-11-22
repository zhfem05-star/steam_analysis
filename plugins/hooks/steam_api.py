"""
Steam API 통합 Hook

Steam Web API(공식) 및 Store API(비공식) 호출을 담당한다
API key 관리, rate limit 대기, 페이지네이션을 한 곳에서 처리
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator

import requests
from airflow.models import Variable
from utils.review_types import ReviewRecord

log = logging.getLogger(__name__)

# Steam API 기본 설정
_BASE_URL_API = "https://api.steampowered.com"
_BASE_URL_STORE = "https://store.steampowered.com"
_RATE_LIMIT_SEC = 0.5
_TIMEOUT = 30


class SteamApiHook:
    """
    Steam API 통합 Hook

    공식/비공식 API를 메서드별로 제공하며,
    rate limit·에러 처리·페이지네이션을 내부 처리
    """

    def __init__(self, api_key: str | None = None) -> None:
        """
        :param api_key: Steam Web API Key
                        None이면 Airflow Variable 'steam_api_key'에서 조회
        """
        self._api_key = api_key

    @property
    def api_key(self) -> str:
        if self._api_key is None:
            self._api_key = Variable.get("steam_api_key")
        return self._api_key

    # ── 내부 공통 메서드 ─────────────────────────────

    @staticmethod
    def _masked_params(params: dict) -> dict:
        """로그용 파라미터 사본 (API key 값 가림)"""
        return {k: ("***" if k == "key" else v) for k, v in params.items()}

    def _redact(self, text: str) -> str:
        """예외 메시지에 포함된 요청 URL의 API key 값 가림"""
        key = self._api_key
        return text.replace(key, "***") if key else text

    def _request(self, url: str, params: dict | None = None) -> dict:
        """
        HTTP GET 요청 + 에러 로깅

        - 로그와 예외 메시지에서 API key 값을 가림
        - requests 예외는 URL에 key가 포함되므로 원본 예외 연결 없이 같은 형으로 다시 발생

        :raises requests.ConnectionError: 네트워크 연결 실패
        :raises requests.Timeout: 응답 시간 초과
        :raises requests.HTTPError: 2xx 이외 응답
        """
        params = params or {}
        log.info("Steam API 요청: %s  params=%s", url, self._masked_params(params))

        try:
            resp = requests.get(url, params=params, timeout=_TIMEOUT)
        except requests.Timeout as error:
            log.error("요청 타임아웃(%ss): %s", _TIMEOUT, url)
            raise requests.Timeout(self._redact(str(error))) from None
        except requests.ConnectionError as error:
            log.error("네트워크 연결 실패: %s", url)
            raise requests.ConnectionError(self._redact(str(error))) from None

        if not resp.ok:
            log.error(
                "API 응답 에러: status=%s  url=%s  body=%s",
                resp.status_code,
                url,
                resp.text[:500],
            )
            raise requests.HTTPError(
                f"Steam API 응답 오류: status={resp.status_code}, url={url}",
                response=resp,
            )

        return resp.json()

    def _wait(self, seconds: float = _RATE_LIMIT_SEC) -> None:
        """
        Rate limit 대기
        """
        time.sleep(seconds)

    # ── 공식 API: IStoreService ───────────────────────

    def iter_app_list_pages(
        self,
        max_results: int = 50000,
        include_games: bool = True,
        include_dlc: bool = False,
        include_software: bool = False,
    ) -> Iterator[list[dict]]:
        """
        전체 Steam 앱 목록을 페이지 단위로 순회

        - 영상·하드웨어는 항상 제외
        - 페이지 사이에 rate limit 대기

        :yield: 페이지별 앱 목록. 항목 필드는 appid, name, last_modified, price_change_number
        :raises RuntimeError: 다음 페이지가 있다는 응답에 last_appid가 없음
        """
        url = f"{_BASE_URL_API}/IStoreService/GetAppList/v1/"
        last_appid = 0
        received = 0

        while True:
            params = {
                "key": self.api_key,
                "max_results": max_results,
                "last_appid": last_appid,
                "include_games": include_games,
                "include_dlc": include_dlc,
                "include_software": include_software,
                "include_videos": False,
                "include_hardware": False,
            }

            data = self._request(url, params)
            response = data.get("response", {})
            apps = response.get("apps", [])
            received += len(apps)

            log.info("앱 %d개 수신 (누적: %d)", len(apps), received)
            yield apps

            if not response.get("have_more_results"):
                return

            if "last_appid" not in response:
                raise RuntimeError(
                    f"GetAppList 다음 페이지 기준 누락: 누적 수신={received}, 직전 last_appid={last_appid}"
                )
            last_appid = response["last_appid"]
            self._wait()

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
        return [
            app
            for page in self.iter_app_list_pages(
                max_results=max_results,
                include_games=include_games,
                include_dlc=include_dlc,
                include_software=include_software,
            )
            for app in page
        ]

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
        date_range_type: str | None = None,
        start_date: int | None = None,
        end_date: int | None = None,
    ) -> tuple[list[dict], dict[str, str]]:
        """
        게임 리뷰 조회 (다국어 + cursor 기반 증분 수집)

        :param cursors:     언어별 시작 cursor. {"korean": "AoJ...", "english": "AoJ..."}
                            None 또는 키 누락 시 해당 언어를 처음부터("*") 수집
        :param num_reviews: 언어별 최대 수집 수. None이면 전체 수집
        :param languages:   수집할 언어 목록. 기본값 ["korean", "english"]
        :param date_range_type: 날짜 범위를 포함시킬지 여부에 대한 파라미터
        :param start_date: 수집 시작 날짜
        :param end_date: 수집 종료 날짜
        :return: (중복 제거된 리뷰 리스트, 언어별 마지막 cursor 딕셔너리)
                 next_cursors는 tracked_games.review_cursors에 저장하여 다음 실행 시 재사용
        """
        if languages is None:
            languages = ["korean", "english"]
        if cursors is None:
            cursors = {}

        seen_ids: set[str | int | None] = set()
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
                date_range_type=date_range_type,
                start_date=start_date,
                end_date=end_date,
            )
            next_cursors[language] = last_cursor
            for review in lang_reviews:
                rid = review.get("recommendationid")
                if rid not in seen_ids:
                    seen_ids.add(rid)
                    all_reviews.append(review)
            log.info(
                "appid=%s language=%s 수집 완료: %d개 (전체 누적 %d개)",
                appid,
                language,
                len(lang_reviews),
                len(all_reviews),
            )

        return all_reviews, next_cursors

    def _get_reviews_for_language(
        self,
        appid: int,
        language: str,
        start_cursor: str = "*",
        num_reviews: int | None = None,
        filter_type: str = "recent",
        date_range_type: str | None = None,
        start_date: int | None = None,
        end_date: int | None = None,
    ) -> tuple[list[dict], str]:
        """
        단일 언어 리뷰 페이지네이션

        :return: (리뷰 리스트, 마지막으로 받은 cursor)
                 cursor는 다음 실행의 start_cursor로 사용하여 이어서 수집 가능
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
                "date_range_type": date_range_type,
                "start_date": start_date,
                "end_date": end_date,
            }
            if (start_date or end_date) and not params["date_range_type"]:
                log.info("date_range_type 미설정")
            elif params["date_range_type"] and (
                params["start_date"] is None or params["end_date"] is None
            ):
                log.warning(
                    # start_date나 end_date 파라미터 설정 안 했을 경우
                    "현재 미설정 파라미터: %s, %s",
                    start_date,
                    end_date,
                )

            try:
                data = self._request(url, params)
            except Exception as e:
                # 네트워크/타임아웃 오류 — 마지막으로 성공한 cursor까지만 반환
                log.error(
                    "페이지 요청 실패, 수집 중단: appid=%s language=%s cursor=%s error=%s",
                    appid,
                    language,
                    cursor,
                    e,
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
            log.info(
                "리뷰 %d개 수신 (누적: %d, 언어=%s)", len(batch), len(reviews), language
            )
            self._wait()

        return reviews, last_cursor

    def iter_review_pages(
        self,
        appid: int,
        language: str,
        start_cursor: str = "*",
        filter_type: str = "recent",
        date_range_type: str | None = None,
        start_date: int | None = None,
        end_date: int | None = None,
    ) -> Iterator[tuple[list[ReviewRecord], str]]:
        """
        단일 언어 리뷰를 페이지 단위로 yield하는 제너레이터

        빈 날짜 구간을 포함해 모든 요청 전에 대기하여 호출 간격을 지킨다

        :param date_range_type: 날짜 범위 적용 방식 ("include": 지정 범위 안의 리뷰 조회)
        :param start_date: 수집 시작 시각 (초 단위 Unix timestamp)
        :param end_date: 수집 종료 시각 (초 단위 Unix timestamp)
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
            params.update(
                {
                    key: value
                    for key, value in {
                        "date_range_type": date_range_type,
                        "start_date": start_date,
                        "end_date": end_date,
                    }.items()
                    if value is not None
                }
            )

            self._wait()  # 빈 날짜 구간이나 cursor 초기화 후에도 요청 간격을 지킨다.
            try:
                data = self._request(url, params)
            except Exception as e:
                log.error(
                    "페이지 요청 실패, 중단: appid=%s language=%s cursor=%s error=%s",
                    appid,
                    language,
                    cursor,
                    e,
                )
                raise

            if data.get("success") != 1:
                log.warning("리뷰 조회 실패: appid=%s language=%s", appid, language)
                raise RuntimeError(
                    f"Steam reviews failed: appid={appid} language={language}"
                )

            batch = data.get("reviews")
            if not isinstance(batch, list):
                raise ValueError("Steam reviews response has no valid reviews list")
            if not batch:
                return

            next_cursor = data.get("cursor")
            if not next_cursor or next_cursor == cursor:
                raise RuntimeError("Steam review cursor did not advance")
            log.info(
                "페이지 수신: appid=%s language=%s 리뷰=%d개",
                appid,
                language,
                len(batch),
            )

            yield batch, next_cursor  # consumer가 이 데이터를 처리하는 동안

            cursor = next_cursor

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

    def iter_store_query_pages(
        self,
        query_name: str,
        filters: dict,
        context: dict,
        data_request: dict,
        page_size: int = 1000,
        sort: int = 1,
    ) -> Iterator[tuple[list[dict], int]]:
        """
        IStoreQueryService/Query 전체 결과를 페이지 단위로 순회

        - 이름순(sort=1) 순회에서 중복·누락 없음을 2026-11-22 전체 201,011건으로 확인
        - 페이지 사이에 rate limit 대기
        - 끝까지 받지 못한 순회는 완료로 취급하면 안 되므로 빈 페이지가 먼저 오면 예외

        :param query_name: 로깅용 쿼리 이름
        :param filters: query.filters 객체
        :param context: 언어·국가 설정
        :param data_request: 응답에 포함할 데이터
        :param page_size: 페이지당 요청 수 (API 최대 1000)
        :param sort: 정렬 기준
        :yield: (페이지 항목 목록, 응답의 전체 건수)
        :raises RuntimeError: 전체 건수에 도달하기 전에 빈 페이지 수신
        """
        start = 0
        total: int | None = None

        while total is None or start < total:
            data = self.get_query(
                query_name=query_name,
                start=start,
                count=page_size,
                sort=sort,
                filters=filters,
                context=context,
                data_request=data_request,
            )
            response = data.get("response", {})
            total = int(response.get("metadata", {}).get("total_matching_records", 0))
            items = response.get("store_items", [])

            if not items:
                if start >= total:
                    return
                raise RuntimeError(
                    f"Query 순회 중 빈 페이지: query={query_name}, start={start}, total={total}"
                )

            log.info("Query %s: %d건 수신 (%d/%d)", query_name, len(items), start + len(items), total)
            yield items, total

            start += len(items)
            if start < total:
                self._wait()
