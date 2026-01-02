from app.services.resource_providers.base import BaseResourceProvider, ResourceItem
from typing import List
import logging
import httpx
import asyncio
import threading
from functools import lru_cache
import re
import os
import time
import json
from collections import deque
from sqlalchemy.orm import Session
from app.core.settings_helper import get_setting_value

logger = logging.getLogger(__name__)

class QuotaExceededError(Exception):
    """当 API 配额超限时抛出的异常"""
    def __init__(self, message="API 配额已用尽，请稍后再试"):
        self.message = message
        super().__init__(self.message)

class RateLimiter:
    """
    速率限制器 - 限制 nullbr API 请求频率
    限制：每分钟最多 30 次请求，且每次请求间隔至少 1 秒
    """
    def __init__(self, max_requests: int = 30, time_window: int = 60, min_interval: float = 1.0):
        self.max_requests = max_requests
        self.time_window = time_window
        self.min_interval = min_interval
        self.requests = deque()
        self._thread_local = threading.local()
        self.last_request_time = 0
    
    def _get_lock(self):
        try:
            current_loop = asyncio.get_running_loop()
            loop_id = id(current_loop)
        except RuntimeError:
            loop_id = id(threading.current_thread())
        
        if not hasattr(self._thread_local, 'lock') or not hasattr(self._thread_local, 'loop_id') or self._thread_local.loop_id != loop_id:
            self._thread_local.lock = asyncio.Lock()
            self._thread_local.loop_id = loop_id
        
        return self._thread_local.lock
        
    async def acquire(self):
        lock = self._get_lock()
        async with lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            if time_since_last_request < self.min_interval:
                wait_time = self.min_interval - time_since_last_request
                await asyncio.sleep(wait_time)
                current_time = time.time()
            
            while self.requests and current_time - self.requests[0] >= self.time_window:
                self.requests.popleft()
            
            if len(self.requests) >= self.max_requests:
                oldest_request = self.requests[0]
                wait_time = self.time_window - (current_time - oldest_request)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    current_time = time.time()
                    while self.requests and current_time - self.requests[0] >= self.time_window:
                        self.requests.popleft()
            
            self.requests.append(current_time)
            self.last_request_time = current_time

_nullbr_rate_limiter = RateLimiter(max_requests=30, time_window=60)
_thread_local = threading.local()

def get_http_client():
    if not hasattr(_thread_local, "client"):
        _thread_local.client = None
    if not hasattr(_thread_local, "loop_id"):
        _thread_local.loop_id = None
    
    try:
        current_loop = asyncio.get_running_loop()
        current_loop_id = id(current_loop)
    except RuntimeError:
        return httpx.AsyncClient(timeout=30.0, trust_env=False)

    if _thread_local.client is None or _thread_local.client.is_closed or _thread_local.loop_id != current_loop_id:
        _thread_local.client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            http2=True,
            trust_env=True
        )
        _thread_local.loop_id = current_loop_id
        
    return _thread_local.client

class NullbrService:
    def __init__(self, db: Session, config: dict = None):
        self.db = db
        config = config or {}
        
        raw_url = config.get("api_url") or get_setting_value(db, "RESOURCE_API_URL")
        if not raw_url:
            raw_url = os.environ.get("RESOURCE_API_URL")
        self.base_url = self._normalize_url(raw_url) if raw_url else None
        self.app_id = "o5scAf4FG"
        self.api_key = config.get("api_key") or get_setting_value(db, "RESOURCE_API_KEY")

    def _normalize_url(self, url: str) -> str:
        if not url:
            return ""
        url = url.strip()
        url = url.rstrip('/')
        if not url.startswith(('http://', 'https://')):
            url = f"https://{url}"
        return url

    def _get_headers(self):
        headers = {
            "User-Agent": "CloudSub/v1.0.6"
        }
        if self.app_id:
            headers["X-APP-ID"] = self.app_id
        if self.api_key:
            headers["X-API-KEY"] = self.api_key
        return headers

    async def get_movie_115(self, tmdb_id: int):
        if not self.base_url or not self.app_id:
            return []
        await _nullbr_rate_limiter.acquire()
        url = f"{self.base_url}/movie/{tmdb_id}/115"
        try:
            client = get_http_client()
            response = await client.get(url, headers=self._get_headers())
            if response.status_code == 200:
                items = response.json().get("115", [])
                for item in items:
                    if "share_link" in item:
                        item["url"] = item["share_link"]
                return items
            elif response.status_code == 403:
                raise QuotaExceededError()
            return []
        except QuotaExceededError:
            raise
        except Exception as e:
            logger.error(f"Nullbr get_movie_115 failed: {e}")
            return []

    async def get_movie_magnet(self, tmdb_id: int):
        if not self.base_url or not self.app_id:
            return []
        await _nullbr_rate_limiter.acquire()
        url = f"{self.base_url}/movie/{tmdb_id}/magnet"
        try:
            client = get_http_client()
            response = await client.get(url, headers=self._get_headers())
            if response.status_code == 200:
                items = response.json().get("magnet", [])
                for item in items:
                    if "magnet" in item:
                        item["url"] = item["magnet"]
                return items
            elif response.status_code == 403:
                raise QuotaExceededError()
            return []
        except QuotaExceededError:
            raise
        except Exception as e:
            logger.error(f"Nullbr get_movie_magnet failed: {e}")
            return []
    
    async def get_movie_ed2k(self, tmdb_id: int):
        if not self.base_url or not self.app_id:
            return []
        await _nullbr_rate_limiter.acquire()
        url = f"{self.base_url}/movie/{tmdb_id}/ed2k"
        try:
            client = get_http_client()
            response = await client.get(url, headers=self._get_headers())
            if response.status_code == 200:
                items = response.json().get("ed2k", [])
                for item in items:
                    if "ed2k" in item:
                        item["url"] = item["ed2k"]
                return items
            elif response.status_code == 403:
                raise QuotaExceededError()
            return []
        except QuotaExceededError:
            raise
        except Exception as e:
            logger.error(f"Nullbr get_movie_ed2k failed: {e}")
            return []

    async def get_tv_115(self, tmdb_id: int, season: int, episode: int):
        if not self.base_url or not self.app_id:
            return []
        await _nullbr_rate_limiter.acquire()
        url = f"{self.base_url}/tv/{tmdb_id}/115"
        try:
            client = get_http_client()
            response = await client.get(url, headers=self._get_headers())
            if response.status_code == 200:
                items = response.json().get("115", [])
                for item in items:
                    if "share_link" in item:
                        item["url"] = item["share_link"]
                return items
            elif response.status_code == 403:
                raise QuotaExceededError()
            return []
        except QuotaExceededError:
            raise
        except Exception as e:
            logger.error(f"Nullbr get_tv_115 failed: {e}")
            return []

    async def get_tv_season_magnet(self, tmdb_id: int, season: int):
        if not self.base_url or not self.app_id:
            return []
        await _nullbr_rate_limiter.acquire()
        url = f"{self.base_url}/tv/{tmdb_id}/season/{season}/magnet"
        try:
            client = get_http_client()
            response = await client.get(url, headers=self._get_headers())
            if response.status_code == 200:
                items = response.json().get("magnet", [])
                for item in items:
                    if "magnet" in item:
                        item["url"] = item["magnet"]
                return items
            elif response.status_code == 403:
                raise QuotaExceededError()
            return []
        except QuotaExceededError:
            raise
        except Exception as e:
            logger.error(f"Nullbr get_tv_season_magnet failed: {e}")
            return []

    async def get_tv_magnet(self, tmdb_id: int, season: int, episode: int):
        if not self.base_url or not self.app_id:
            return []
        await _nullbr_rate_limiter.acquire()
        url = f"{self.base_url}/tv/{tmdb_id}/season/{season}/episode/{episode}/magnet"
        try:
            client = get_http_client()
            response = await client.get(url, headers=self._get_headers())
            if response.status_code == 200:
                items = response.json().get("magnet", [])
                for item in items:
                    if "magnet" in item:
                        item["url"] = item["magnet"]
                return items
            elif response.status_code == 403:
                raise QuotaExceededError()
            return []
        except QuotaExceededError:
            raise
        except Exception as e:
            logger.error(f"Nullbr get_tv_magnet failed: {e}")
            return []

    async def get_tv_ed2k(self, tmdb_id: int, season: int, episode: int):
        if not self.base_url or not self.app_id:
            return []
        await _nullbr_rate_limiter.acquire()
        url = f"{self.base_url}/tv/{tmdb_id}/season/{season}/episode/{episode}/ed2k"
        try:
            client = get_http_client()
            response = await client.get(url, headers=self._get_headers())
            if response.status_code == 200:
                items = response.json().get("ed2k", [])
                for item in items:
                    if "ed2k" in item:
                        item["url"] = item["ed2k"]
                return items
            elif response.status_code == 403:
                raise QuotaExceededError()
            return []
        except QuotaExceededError:
            raise
        except Exception as e:
            logger.error(f"Nullbr get_tv_ed2k failed: {e}")
            return []

class NullbrProvider(BaseResourceProvider):
    name = "nullbr"
    description = "Nullbr"
    cron = "0 */4 * * *"

    @classmethod
    def get_config_schema(cls):
        return {
            "fields": [
                {
                    "name": "api_url",
                    "label": "API URL",
                    "type": "text",
                    "description": "Nullbr API 地址",
                    "required": True
                },
                {
                    "name": "api_key",
                    "label": "API 密钥",
                    "type": "text",
                    "description": "Nullbr API 密钥",
                    "required": True
                },
                {
                    "name": "cron",
                    "label": "Cron 表达式",
                    "type": "text",
                    "description": "允许搜索该来源的最小周期 (例如 0 */4 * * * 为最少间隔4小时才能搜索一次)",
                    "required": False,
                    "default": cls.cron
                }
            ]
        }

    async def search(self, keyword: str, tmdb_id: int, media_type: str, season: int = None, episode: int = None, **kwargs) -> List[ResourceItem]:
        db = kwargs.get('db')
        if not db:
            logger.error("Database session not provided to NullbrProvider")
            return []

        config_json = get_setting_value(db, f"PROVIDER_CONFIG_{self.name}")
        config = json.loads(config_json) if config_json else {}

        service = NullbrService(db, config)
        results = []
        
        try:
            l115 = []
            mags = []
            ed2ks = []

            if media_type == "movie":
                l115 = await service.get_movie_115(tmdb_id)
                mags = await service.get_movie_magnet(tmdb_id)
                ed2ks = await service.get_movie_ed2k(tmdb_id)
            elif media_type == "tv":
                if season is not None:
                    if episode is not None:
                        l115 = await service.get_tv_115(tmdb_id, season, episode)
                        mags = await service.get_tv_magnet(tmdb_id, season, episode)
                        ed2ks = await service.get_tv_ed2k(tmdb_id, season, episode)
                    else:
                        l115 = await service.get_tv_115(tmdb_id, season, None)
                        mags = await service.get_tv_season_magnet(tmdb_id, season)
                        ed2ks = []
                else:
                     # Default to S01E01 if no season specified
                     l115 = await service.get_tv_115(tmdb_id, 1, 1)
                     mags = await service.get_tv_magnet(tmdb_id, 1, 1)
                     ed2ks = await service.get_tv_ed2k(tmdb_id, 1, 1)

            for item in l115:
                results.append(ResourceItem(
                    title=item.get("title", "Unknown"),
                    size=item.get("size"),
                    link=item.get("share_link", ""),
                    type="115",
                    source="NULLBR",
                    metadata=item
                ))
            
            for item in mags:
                results.append(ResourceItem(
                    title=item.get("name", item.get("title", "Unknown")),
                    size=item.get("size"),
                    link=item.get("magnet", ""),
                    type="magnet",
                    source="NULLBR",
                    metadata=item
                ))
                
            for item in ed2ks:
                results.append(ResourceItem(
                    title=item.get("name", item.get("title", "Unknown")),
                    size=item.get("size"),
                    link=item.get("ed2k", ""),
                    type="ed2k",
                    source="NULLBR",
                    metadata=item
                ))

        except Exception as e:
            logger.error(f"Nullbr search failed: {e}")
            
        return results
