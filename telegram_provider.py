from app.services.resource_providers.base import BaseResourceProvider, ResourceItem
from typing import List, Dict
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
from bs4 import BeautifulSoup
from app.services.p115 import P115Service
from app.utils.resource_parser import parse_resource_meta

logger = logging.getLogger(__name__)

class TelegramChannelService:
    """
    Telegram 频道搜索服务
    """
    def __init__(self, db: Session, config: dict = None):
        self.db = db
        self.config = config or {}
        self.base_url = "https://t.me/s"
        self.channels = self._get_channels()
        # 初始化 P115 服务用于解析文件名
        try:
            self.p115_service = P115Service(db)
            self.p115_enabled = bool(self.p115_service.cookie)
        except Exception as e:
            logger.warning(f"P115 service initialization failed: {e}")
            self.p115_service = None
            self.p115_enabled = False

    def _get_channels(self) -> List[Dict]:
        if self.config and "channels" in self.config:
             channels = self.config["channels"]
             if isinstance(channels, str):
                 try:
                     return json.loads(channels)
                 except:
                     return []
             return channels

        channels_json = get_setting_value(self.db, "TELEGRAM_CHANNELS")
        if not channels_json:
            return []
        try:
            return json.loads(channels_json)
        except json.JSONDecodeError:
            logger.error("Failed to parse TELEGRAM_CHANNELS setting")
            return []

    async def search(self, keyword: str) -> List[Dict]:
        if not self.channels:
            return []

        tasks = []
        for channel in self.channels:
            channel_id = channel.get("id")
            if channel_id:
                tasks.append(self._search_in_channel(channel_id, keyword, keyword))
        
        results = await asyncio.gather(*tasks)
        
        all_items = []
        for channel_results in results:
            all_items.extend(channel_results)
            
        all_items.sort(key=lambda x: x.get("time", "") or "", reverse=True)
            
        return all_items

    async def _search_in_channel(self, channel_id: str, keyword: str, title: str) -> List[Dict]:
        url = f"{self.base_url}/{channel_id}"
        params = {"q": keyword}
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        try:
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                response = await client.get(url, params=params, headers=headers)
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch telegram channel {channel_id}: {response.status_code}")
                    return []
                
                return self._parse_html(response.text, channel_id, title)
        except Exception as e:
            logger.error(f"Error searching telegram channel {channel_id}: {type(e).__name__} - {e}", exc_info=True)
            return []

    def _parse_html(self, html: str, channel_id: str, title: str) -> List[Dict]:
        soup = BeautifulSoup(html, 'html.parser')
        items = []
        
        messages = soup.select('.tgme_widget_message_wrap')
        messages.reverse()
        
        for message in messages:
            try:
                text_elem = message.select_one('.tgme_widget_message_text')
                if not text_elem:
                    continue
                text_content = text_elem.get_text(separator="\n")
                raw_html = str(text_elem)
                
                links = self._extract_115_links(raw_html)
                
                if not links:
                    continue
                
                time_elem = message.select_one('.tgme_widget_message_date')
                msg_time = time_elem.get('datetime') if time_elem else ""
                
                for link in links:
                    real_file_name = None
                    real_file_size = None
                    p115_parse_failed = False
                    
                    if self.p115_enabled and self.p115_service:
                        try:
                            result = self.p115_service.get_share_first_level(link)
                            if result.get("success") and result.get("name"):
                                real_file_name = result.get("name")
                                real_file_size = result.get("size")
                            else:
                                p115_parse_failed = True
                        except Exception as e:
                            p115_parse_failed = True
                    
                    if self.p115_enabled and p115_parse_failed:
                        continue
                    
                    parsed_meta = parse_resource_meta(
                        title=title,
                        content=text_content,
                        file_name_from_drive=real_file_name
                    )
                    
                    display_name = title
                    quality_info = []
                    
                    if real_file_name and parsed_meta.file_name_candidates:
                        display_name = parsed_meta.file_name_candidates[0]
                    else:
                        if parsed_meta.resolution:
                            quality_info.append(parsed_meta.resolution)
                        
                        meaningful_tags = ['HDR', 'HDR10', 'Dolby Vision', 'SDR', 'Remux', 
                                           'BluRay', 'WEB-DL', 'WEBRip', 'H.265', 'H.264']
                        if parsed_meta.tags:
                            for tag in parsed_meta.tags:
                                if tag in meaningful_tags:
                                    quality_info.append(tag)
                                    if len(quality_info) >= 4:
                                        break
                        
                        if quality_info:
                            display_name = f"{title} [{', '.join(quality_info)}]"
                    
                    format_value = parsed_meta.format if parsed_meta.format not in ['unknown', 'other', None] else None
                    
                    display_size = ""
                    if real_file_size:
                        display_size = self._format_size(real_file_size)
                    else:
                        display_size = self._extract_size(text_content)

                    items.append({
                        "title": display_name,
                        "name": display_name,
                        "share_link": link,
                        "url": link,
                        "size": display_size,
                        "resolution": parsed_meta.resolution or self._extract_resolution(text_content),
                        "quality": "",
                        "source": f"Telegram: {channel_id}",
                        "time": msg_time,
                        "file_name_candidates": parsed_meta.file_name_candidates,
                        "ext": parsed_meta.ext,
                        "format": format_value,
                        "tags": parsed_meta.tags,
                    })
                    
            except Exception as e:
                logger.error(f"Error parsing message in {channel_id}: {e}")
                continue
                
        return items

    def _extract_115_links(self, html: str) -> List[str]:
        links = []
        regex = r'https?://(?:115\.com|115cdn\.com|anxia\.com)/s/[a-zA-Z0-9]+(?:\?password=[a-zA-Z0-9]+)?'
        found = re.findall(regex, html)
        links.extend(found)
        return list(set(links))

    def _extract_size(self, text: str) -> str:
        regex = r'(\d+(?:\.\d+)?\s*(?:GB|MB|TB|KB))'
        match = re.search(regex, text, re.IGNORECASE)
        if match:
            return match.group(1)
        return ""

    def _format_size(self, size_bytes: int) -> str:
        if not size_bytes:
            return ""
        try:
            size = float(size_bytes)
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if size < 1024.0:
                    return f"{size:.2f} {unit}"
                size /= 1024.0
            return f"{size:.2f} PB"
        except (ValueError, TypeError):
            return ""

    def _extract_resolution(self, text: str) -> str:
        resolutions = [r'2160[pP]', r'1080[pP]', r'720[pP]', r'4K', r'8K']
        for res_pattern in resolutions:
            match = re.search(res_pattern, text)
            if match:
                return match.group(0)
        return ""

class TelegramProvider(BaseResourceProvider):
    name = "telegram"
    description = "Telegram"
    cron = "0 * * * *"

    @classmethod
    def get_config_schema(cls):
        return {
            "fields": [
                {
                    "name": "channels",
                    "label": "频道列表",
                    "type": "list",
                    "description": "要搜索的 Telegram 频道列表",
                    "item_schema": [
                        {"name": "id", "label": "频道 ID", "type": "text"},
                        {"name": "name", "label": "频道名称", "type": "text"}
                    ]
                },
                {
                    "name": "cron",
                    "label": "Cron 表达式",
                    "type": "text",
                    "description": "允许搜索该来源的最小周期 (例如 0 * * * * 为最少间隔1小时才能搜索一次)",
                    "required": False,
                    "default": cls.cron
                }
            ]
        }

    async def search(self, keyword: str, tmdb_id: int, media_type: str, season: int = None, episode: int = None, **kwargs) -> List[ResourceItem]:
        db = kwargs.get('db')
        if not db:
            logger.error("Database session not provided to TelegramProvider")
            return []

        if not keyword:
            return []

        config_json = get_setting_value(db, f"PROVIDER_CONFIG_{self.name}")
        config = json.loads(config_json) if config_json else {}

        service = TelegramChannelService(db, config)
        results = []
        
        try:
            tg_items = await service.search(keyword)
            
            for item in tg_items:
                share_link = item.get("share_link", "")
                if "115.com" in share_link or "115cdn.com" in share_link or "anxia.com" in share_link:
                    results.append(ResourceItem(
                        title=item.get("title", "Unknown"),
                        size=item.get("size"),
                        link=share_link,
                        type="115",
                        source="TELEGRAM",
                        metadata=item
                    ))
                
        except Exception as e:
            logger.error(f"Telegram search failed: {e}")
            
        return results
