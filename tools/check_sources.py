#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV 直播源聚合 + 可用性校验脚本 (v2 - 多源聚合版)

数据来源：
  1. 本地文件 tv/iptv4.txt、tv/iptv6.txt
     （vbskycn/iptv 原始格式："分组,#genre#" + "频道名,url1;url2"）
  2. 远程 iptv-org 开源库的中国大陆频道列表（标准 m3u 格式）
     https://iptv-org.github.io/iptv/countries/cn.m3u
     （iptv-org 是国际上维护最活跃、内容审核最规范的开源 IPTV 源项目之一）

处理流程：
  抓取所有来源 -> 统一解析成 (分组, 频道名, [url,...]) -> 按 URL 去重
  -> 并发校验每个 URL 是否可连通 -> 写出 *_checked.txt / *_checked.m3u

设计原则（延续 v1）：
- 不修改原始文件 tv/iptv4.txt 等，避免和 sync-upstream.yml 的强制同步互相覆盖。
- 非 http(s) 协议（udp:// rtmp:// rtsp:// 等）无法用普通 HTTP 请求测试，
  为避免"测不了就当作失效删掉"造成误伤，这类地址直接保留、不校验。
- 同一个 URL 在多个来源里重复出现时只保留一份，避免重复测试浪费时间。
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

try:
    import aiohttp
except ImportError:
    print("缺少依赖，请先 pip install aiohttp", file=sys.stderr)
    sys.exit(1)

# ---------- 配置 ----------
LOCAL_SOURCE_FILES = ["tv/iptv4.txt", "tv/iptv6.txt"]

# 远程数据源：(名字, url, 格式)。格式目前支持 "m3u"。
# 想接入更多源，往这个列表里加一行即可。
REMOTE_SOURCES = [
    ("iptv-org-cn", "https://iptv-org.github.io/iptv/countries/cn.m3u", "m3u"),
]

TIMEOUT_SECONDS = 8
CONCURRENCY = 40
READ_BYTES = 2048          # 只读一点数据确认连接可用，不下载整段流
HTTP_SCHEMES = ("http://", "https://")
OUTPUT_STEM = "aggregated"  # 产出文件名：tv/aggregated_checked.txt / .m3u
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
    """解析标准 m3u，返回 [(分组, 频道名, url), ...]"""
    items = []
    lines = text.splitlines()
    pending_group = "未分组"
    pending_name = None
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#EXTM3U"):
            continue
        if line.startswith("#EXTINF"):
            # group-title="xxx" 和逗号后面的频道名
            group = "未分组"
            if 'group-title="' in line:
                try:
                    group = line.split('group-title="', 1)[1].split('"', 1)[0]
                except IndexError:
                    pass
            name = line.rsplit(",", 1)[-1].strip()
            pending_group, pending_name = group, name
            continue
        if line.startswith("#"):
            continue  # 其他 #EXTVLCOPT 等注释行，跳过
        # 到这里是 URL 行
        if pending_name:
            items.append((pending_group, pending_name, line))
            pending_name = None
    return items


def parse_vbskycn_txt(text: str):
    """解析 vbskycn 格式，返回 [(分组, 频道名, url), ...]（一行多个url拆开成多条）"""
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


# ============ 校验 ============

async def check_url(session: "aiohttp.ClientSession", url: str, sem: asyncio.Semaphore) -> bool:
    if not url.startswith(HTTP_SCHEMES):
        return True  # 无法用 HTTP 测试的协议，不做判断，保留原样
    async with sem:
        try:
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
            async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
                if resp.status >= 400:
                    return False
                await resp.content.read(READ_BYTES)
                return True
        except Exception:
            return False


async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)

    all_items = []          # (分组, 频道名, url)
    per_source_count = {}   # 来源名 -> 抓到的条目数

    async with aiohttp.ClientSession(connector=connector) as session:
        # 1) 本地文件
        for f in LOCAL_SOURCE_FILES:
            path = Path(f)
            if not path.exists():
                print(f"[跳过] 找不到本地文件 {f}")
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            items = parse_vbskycn_txt(text)
            all_items.extend(items)
            per_source_count[f] = len(items)

        # 2) 远程数据源
        for name, url, fmt in REMOTE_SOURCES:
            text = await fetch_text(session, url)
            if text is None:
                per_source_count[name] = 0
                continue
            items = parse_m3u(text) if fmt == "m3u" else []
            all_items.extend(items)
            per_source_count[name] = len(items)

        # 3) 按 URL 去重（同一条流地址只保留第一次出现的分组/名字）
        seen_urls = set()
        deduped = []
        for group, name, url in all_items:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            deduped.append((group, name, url))

        total_before = len(all_items)
        total_after_dedup = len(deduped)

        # 4) 并发校验
        results = await asyncio.gather(*[check_url(session, url, sem) for _, _, url in deduped])
        passed_items = [item for item, ok in zip(deduped, results) if ok]

    # 5) 按分组整理输出，分组内保持原始出现顺序
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
        txt_lines.append("")  # 空行分隔分组

    out_txt = Path("tv") / f"{OUTPUT_STEM}_checked.txt"
    out_m3u = Path("tv") / f"{OUTPUT_STEM}_checked.m3u"
    out_txt.write_text("\n".join(txt_lines), encoding="utf-8")
    out_m3u.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

    # 6) 摘要
    lines = [
        f"## 🔍 多源聚合校验结果 ({datetime.now(timezone.utc).isoformat()})",
        "",
        "**各来源抓取条目数：**",
    ]
    for src, cnt in per_source_count.items():
        lines.append(f"- {src}: {cnt} 条")
    lines += [
        "",
        f"**合并前总数**：{total_before}",
        f"**按URL去重后**：{total_after_dedup}",
        f"**校验通过**：{len(passed_items)}",
        f"**剔除失效**：{total_after_dedup - len(passed_items)}",
        "",
        f"输出文件：`{out_txt}` / `{out_m3u}`",
    ]
    summary_text = "\n".join(lines)
    Path("check_summary.md").write_text(summary_text, encoding="utf-8")
    print(summary_text)


if __name__ == "__main__":
    asyncio.run(main())
