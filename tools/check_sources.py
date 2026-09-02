#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV 直播源可用性校验脚本

读取 tv/iptv4.txt、tv/iptv6.txt（vbskycn/iptv 项目原始格式：
    分组名,#genre#
    频道名,url1;url2;...
    (空行分隔分组)
），对每个 URL 发起一次轻量 HTTP 请求测试是否可连通/能取到内容，
把校验通过的频道写到 *_checked.txt 和 *_checked.m3u。

设计原则：
- 不修改原始文件，避免和 sync-upstream.yml 的强制同步互相覆盖。
- 非 http(s) 协议（udp:// rtmp:// rtsp:// 等）无法用普通 HTTP 请求测试，
  为避免"测不了就当作失效删掉"造成误伤，这类地址直接保留、不校验。
- 一个频道名下有多个用 ; 分隔的备用地址时，只要有一个测试通过就保留
  （保留那个测试通过的地址；多个都通过则都保留）。
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
SOURCE_FILES = ["tv/iptv4.txt", "tv/iptv6.txt"]
TIMEOUT_SECONDS = 8
CONCURRENCY = 40
READ_BYTES = 2048          # 只读一点数据确认连接可用，不下载整段流
HTTP_SCHEMES = ("http://", "https://")
# --------------------------


async def check_url(session: "aiohttp.ClientSession", url: str, sem: asyncio.Semaphore) -> bool:
    """测试单个 URL 是否可连通。非 http(s) 协议直接视为'跳过校验，保留'。"""
    if not url.startswith(HTTP_SCHEMES):
        return True  # 无法用 HTTP 测试的协议，不做判断，保留原样

    async with sem:
        try:
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
            async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
                if resp.status >= 400:
                    return False
                # 读一小段确认真的有数据流回来，而不是空 200
                await resp.content.read(READ_BYTES)
                return True
        except Exception:
            return False


async def check_channel_urls(session, name: str, urls: list[str], sem) -> list[str]:
    """一个频道可能有多个 ; 分隔的备用地址，逐个测，返回通过的地址列表。"""
    results = await asyncio.gather(*[check_url(session, u, sem) for u in urls])
    return [u for u, ok in zip(urls, results) if ok]


def parse_lines(text: str):
    """按原始格式解析成 [(kind, payload), ...]
    kind: 'group' -> payload=分组名
          'blank' -> payload=None
          'channel' -> payload=(频道名, [url,...])
    """
    items = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            items.append(("blank", None))
            continue
        if line.endswith("#genre#"):
            group_name = line.split(",", 1)[0]
            items.append(("group", group_name))
            continue
        if "," not in line:
            continue  # 格式不认识的行，跳过
        name, url_part = line.split(",", 1)
        urls = [u.strip() for u in url_part.split(";") if u.strip()]
        if urls:
            items.append(("channel", (name.strip(), urls)))
    return items


async def process_file(session, sem, src_path: Path):
    if not src_path.exists():
        print(f"[跳过] 找不到 {src_path}")
        return None

    text = src_path.read_text(encoding="utf-8", errors="ignore")
    items = parse_lines(text)

    txt_lines = []
    m3u_lines = ["#EXTM3U"]
    current_group = ""
    total = passed = 0

    for kind, payload in items:
        if kind == "blank":
            txt_lines.append("")
            continue
        if kind == "group":
            current_group = payload
            txt_lines.append(f"{payload},#genre#")
            continue
        # channel
        name, urls = payload
        total += 1
        ok_urls = await check_channel_urls(session, name, urls, sem)
        if not ok_urls:
            continue
        passed += 1
        txt_lines.append(f"{name},{';'.join(ok_urls)}")
        for u in ok_urls:
            m3u_lines.append(f'#EXTINF:-1 group-title="{current_group}",{name}')
            m3u_lines.append(u)

    stem = src_path.stem  # iptv4 / iptv6
    out_txt = src_path.with_name(f"{stem}_checked.txt")
    out_m3u = src_path.with_name(f"{stem}_checked.m3u")
    out_txt.write_text("\n".join(txt_lines) + "\n", encoding="utf-8")
    out_m3u.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

    print(f"[{src_path.name}] 频道总数 {total}，通过校验 {passed}，"
          f"失效剔除 {total - passed} -> 写入 {out_txt.name} / {out_m3u.name}")
    return src_path.name, total, passed


async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    summary = []
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for f in SOURCE_FILES:
            result = await process_file(session, sem, Path(f))
            if result:
                summary.append(result)

    # 写一份摘要给 GitHub Actions 的 Step Summary（本地运行时忽略即可）
    summary_path = Path("check_summary.md")
    lines = [f"## 🔍 直播源校验结果 ({datetime.now(timezone.utc).isoformat()})", ""]
    for name, total, passed in summary:
        lines.append(f"- **{name}**: {passed}/{total} 通过 (剔除 {total - passed} 个失效源)")
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    asyncio.run(main())
