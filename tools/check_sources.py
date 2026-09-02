#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV 直播源聚合 + 可用性校验脚本 (v4 - 真实测速版)

数据来源：
  1. 本地文件 tv/iptv4.txt、tv/iptv6.txt
     （vbskycn/iptv 原始格式："分组,#genre#" + "频道名,url1;url2"）
  2. 远程 iptv-org 开源库的中国大陆频道列表（标准 m3u 格式）
     https://iptv-org.github.io/iptv/countries/cn.m3u

处理流程：
  抓取所有来源 -> 统一解析成 (分组, 频道名, url) -> 按 URL 去重
  -> 并发校验每个 URL 是否可连通【且实测下载速率达标】-> 写出 aggregated_checked.txt / .m3u

v4 相比 v2 的核心改动（解决"能连上但播放卡顿"的问题）：
  之前的版本对 .m3u8 地址只读了开头 2KB —— 但 .m3u8 本身只是一份几百字节的
  文本清单（列出真正的视频分片地址），读它本身测不出任何播放速率。
  v4 会先把 .m3u8 清单解析出来，取第一个真实分片（.ts 等）的地址，
  对这个分片做几秒钟的【持续下载测速】，算出真实吞吐率（KB/s），
  低于速率阈值的直接判定失效剔除，这样能过滤掉"连接得上但播放会卡顿"的源。

设计原则（延续之前版本）：
- 不修改原始文件 tv/iptv4.txt 等，避免和 sync-upstream.yml 的强制同步互相覆盖。
- 非 http(s) 协议（udp:// rtmp:// rtsp:// 等）无法用普通 HTTP 请求测试，
  为避免"测不了就当作失效删掉"造成误伤，这类地址直接保留、不校验。
- 同一个 URL 在多个来源里重复出现时只保留一份，避免重复测试浪费时间。
- 测速仍然是"轻量近似"，不是 ffprobe 级别的专业测速（那个交给 Guovin/iptv-api
  那条独立产线负责），但比起 v1/v2 的"能连上就算过"已经准确很多。
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin

try:
    import aiohttp
except ImportError:
    print("缺少依赖，请先 pip install aiohttp", file=sys.stderr)
    sys.exit(1)

# ---------- 配置 ----------
LOCAL_SOURCE_FILES = ["tv/iptv4.txt", "tv/iptv6.txt"]

REMOTE_SOURCES = [
    ("iptv-org-cn", "https://iptv-org.github.io/iptv/countries/cn.m3u", "m3u"),
]

CONCURRENCY = 40
HTTP_SCHEMES = ("http://", "https://")
OUTPUT_STEM = "aggregated"

# --- 测速相关参数 ---
CONNECT_TIMEOUT = 8            # 建立连接 / 拿到首字节的超时（秒）
MANIFEST_MAX_BYTES = 64 * 1024      # .m3u8 清单最大读取字节数（清单本身很小，够用了）
MANIFEST_HOP_LIMIT = 2          # 清单套清单（多码率播放列表）最多往下钻几层
SPEED_TEST_SECONDS = 4.0        # 对真实分片做持续下载测速的时长上限（秒）
SPEED_TEST_MAX_BYTES = 2 * 1024 * 1024   # 测速最多读取的字节数上限（防止分片过大测太久）
MIN_SPEED_KBPS = 100            # 判定"能流畅播放"的最低吞吐率（约等于 0.8Mbps，放宽后保留更多候选做备胎）
MIN_BYTES_REQUIRED = 40 * 1024  # 至少要真收到这么多字节，防止"连上但立刻断流"误判通过
# --------------------------


# ============ 抓取远程数据源 ============

async def fetch_text(session: "aiohttp.ClientSession", url: str) -> str | None:
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                print(f"[警告] 拉取 {url} 失败，状态码 {resp.status}")
                return None
            return await resp.text(errors="ignore")
    except Exception as e:
        print(f"[警告] 拉取 {url} 出错：{e}")
        return None


def parse_m3u(text: str):
    items = []
    pending_group, pending_name = "未分组", None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#EXTM3U"):
            continue
        if line.startswith("#EXTINF"):
            group = "未分组"
            if 'group-title="' in line:
                try:
                    group = line.split('group-title="', 1)[1].split('"', 1)[0]
                except IndexError:
                    pass
            pending_group, pending_name = group, line.rsplit(",", 1)[-1].strip()
            continue
        if line.startswith("#"):
            continue
        if pending_name:
            items.append((pending_group, pending_name, line))
            pending_name = None
    return items


def parse_vbskycn_txt(text: str):
    items = []
    current_group = "未分组"
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.endswith("#genre#"):
            current_group = line.split(",", 1)[0]
            continue
        if "," not in line:
            continue
        name, url_part = line.split(",", 1)
        for u in url_part.split(";"):
            u = u.strip()
            if u:
                items.append((current_group, name.strip(), u))
    return items


# ============ 校验 + 真实测速 ============

async def resolve_real_segment_url(session: "aiohttp.ClientSession", url: str) -> str | None:
    """如果 url 是 .m3u8 清单，递归解析出第一个真实分片地址；否则原样返回。
    解析失败（清单打不开、格式不对等）时返回 None，代表这个源直接判定失效。
    """
    current_url = url
    for _ in range(MANIFEST_HOP_LIMIT + 1):
        if ".m3u8" not in current_url.split("?", 1)[0].lower():
            return current_url  # 不是清单地址，当作直接可读的媒体流

        try:
            timeout = aiohttp.ClientTimeout(total=CONNECT_TIMEOUT)
            async with session.get(current_url, timeout=timeout, allow_redirects=True) as resp:
                if resp.status >= 400:
                    return None
                raw = await resp.content.read(MANIFEST_MAX_BYTES)
                text = raw.decode(errors="ignore")
        except Exception:
            return None

        if "#EXTM3U" not in text:
            return None  # 声称是 m3u8 但内容不对，判失效

        next_line = None
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                next_line = line
                break
        if not next_line:
            return None  # 清单里没有任何分片/子清单，视为失效

        current_url = urljoin(current_url, next_line)

    return None  # 嵌套清单层数太多，放弃


async def measure_throughput_kbps(session: "aiohttp.ClientSession", url: str) -> float | None:
    """对给定地址做几秒钟的持续下载，返回实测 KB/s；失败或数据太少返回 None。"""
    try:
        timeout = aiohttp.ClientTimeout(total=SPEED_TEST_SECONDS + CONNECT_TIMEOUT)
        loop = asyncio.get_event_loop()
        start = loop.time()
        total_bytes = 0
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            if resp.status >= 400:
                return None
            async for chunk in resp.content.iter_chunked(32 * 1024):
                total_bytes += len(chunk)
                elapsed = loop.time() - start
                if elapsed >= SPEED_TEST_SECONDS or total_bytes >= SPEED_TEST_MAX_BYTES:
                    break
        elapsed = max(loop.time() - start, 0.001)
        if total_bytes < MIN_BYTES_REQUIRED:
            return None
        return (total_bytes / 1024) / elapsed
    except Exception:
        return None


async def check_url(session: "aiohttp.ClientSession", url: str, sem: asyncio.Semaphore) -> bool:
    if not url.startswith(HTTP_SCHEMES):
        return True  # 无法用 HTTP 测试的协议，不做判断，保留原样
    async with sem:
        real_url = await resolve_real_segment_url(session, url)
        if real_url is None:
            return False
        speed = await measure_throughput_kbps(session, real_url)
        return speed is not None and speed >= MIN_SPEED_KBPS


async def check_channel_urls(session, name: str, urls: list[str], sem) -> list[str]:
    results = await asyncio.gather(*[check_url(session, u, sem) for u in urls])
    return [u for u, ok in zip(urls, results) if ok]


async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)

    all_items = []
    per_source_count = {}

    async with aiohttp.ClientSession(connector=connector) as session:
        for f in LOCAL_SOURCE_FILES:
            path = Path(f)
            if not path.exists():
                print(f"[跳过] 找不到本地文件 {f}")
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            items = parse_vbskycn_txt(text)
            all_items.extend(items)
            per_source_count[f] = len(items)

        for name, url, fmt in REMOTE_SOURCES:
            text = await fetch_text(session, url)
            if text is None:
                per_source_count[name] = 0
                continue
            items = parse_m3u(text) if fmt == "m3u" else []
            all_items.extend(items)
            per_source_count[name] = len(items)

        seen_urls = set()
        deduped = []
        for group, name, url in all_items:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            deduped.append((group, name, url))

        total_before = len(all_items)
        total_after_dedup = len(deduped)

        results = await asyncio.gather(*[check_url(session, url, sem) for _, _, url in deduped])
        passed_items = [item for item, ok in zip(deduped, results) if ok]

    groups_order = []
    grouped = {}
    for group, name, url in passed_items:
        if group not in grouped:
            grouped[group] = []
            groups_order.append(group)
        grouped[group].append((name, url))

    txt_lines = []
    m3u_lines = ["#EXTM3U"]
    for group in groups_order:
        txt_lines.append(f"{group},#genre#")
        for name, url in grouped[group]:
            txt_lines.append(f"{name},{url}")
            m3u_lines.append(f'#EXTINF:-1 group-title="{group}",{name}')
            m3u_lines.append(url)
        txt_lines.append("")

    out_txt = Path("tv") / f"{OUTPUT_STEM}_checked.txt"
    out_m3u = Path("tv") / f"{OUTPUT_STEM}_checked.m3u"
    out_txt.write_text("\n".join(txt_lines), encoding="utf-8")
    out_m3u.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

    lines = [
        f"## 🔍 多源聚合校验结果 - 真实测速版 ({datetime.now(timezone.utc).isoformat()})",
        "",
        f"测速门槛：≥ {MIN_SPEED_KBPS} KB/s（持续下载 {SPEED_TEST_SECONDS} 秒实测）",
        "",
        "**各来源抓取条目数：**",
    ]
    for src, cnt in per_source_count.items():
        lines.append(f"- {src}: {cnt} 条")
    lines += [
        "",
        f"**合并前总数**：{total_before}",
        f"**按URL去重后**：{total_after_dedup}",
        f"**校验通过（含测速）**：{len(passed_items)}",
        f"**剔除失效/测速不达标**：{total_after_dedup - len(passed_items)}",
        "",
        f"输出文件：`{out_txt}` / `{out_m3u}`",
    ]
    summary_text = "\n".join(lines)
    Path("check_summary.md").write_text(summary_text, encoding="utf-8")
    print(summary_text)


if __name__ == "__main__":
    asyncio.run(main())
