import asyncio
from mcp import Client


async def main():
    async with Client("http://127.0.0.1:8001/mcp") as client:
        tools = await client.list_tools()

        print("Available MCP tools:")
        for tool in tools.tools:
            print("-", tool.name)

        result = await client.call_tool(
            "create_alert",
            {
                "sensor": "Temp1",
                "value": 55,
                "operator": ">",
                "threshold": 50,
                "timestamp": "2026-09-30T10:00:00",
            },
        )

        print("\nMCP tool response:")
        print(result)
        print("\nIs error:", result.is_error)
        print("\nContent:")
        print(result.content)


if __name__ == "__main__":
    asyncio.run(main())
