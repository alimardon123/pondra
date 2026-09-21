#!/usr/bin/env python3
"""Talk to a node over MCP with the official Python SDK (pip install mcp): the handshake, the tools, a
write, a query and the change feed.  mcp_client.py [--url http://127.0.0.1:8080/mcp] [--token TOKEN]"""
import argparse, asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


async def main(a):
    headers = {"Authorization": f"Bearer {a.token}"} if a.token else {}
    async with streamablehttp_client(a.url, headers=headers) as (r, w, _):
        async with ClientSession(r, w) as s:
            init = await s.initialize()
            print("server:", init.serverInfo.name, init.protocolVersion)
            print("tools:", [t.name for t in (await s.list_tools()).tools])
            for tool, args in [("write", {"sql": "CREATE TABLE notes (id BIGINT PRIMARY KEY, body VARCHAR)"}),
                               ("write", {"sql": "INSERT INTO notes VALUES (1, 'hello agent')", "job": "notes-1"}),
                               ("query", {"sql": "SELECT * FROM notes"}),
                               ("changes", {"table": "notes", "after": 0})]:
                out = await s.call_tool(tool, args)
                print(f"{tool}: {'ERROR ' if out.isError else ''}{out.content[0].text}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080/mcp")
    ap.add_argument("--token")
    asyncio.run(main(ap.parse_args()))
