import asyncio
import json
import os

import aiosqlite
from tqdm.auto import tqdm

import config
import discord
import sql


async def loadChannels(conn: aiosqlite.Connection):
    containingFolder = config.settings["SourceFolder"]
    tasks = list()
    for channel in os.scandir(os.path.join(containingFolder, "messages")):
        if channel.is_file():
            continue
        # print("loading", channel.name)
        tasks.append(asyncio.create_task(discord.loadChannel(conn, channel.path)))

    await tqdm.gather(*tasks, desc="Loading channels")
    await conn.commit()

    # update channel names
    with open(os.path.join(containingFolder, "messages", "index.json")) as f:
        data = json.load(f)
        for k, v in data.items():
            await conn.execute(sql.UPDATE_CHANNEL_NAME, (v, k))
    await conn.commit()


async def main():
    async with aiosqlite.connect(sql.DB_PATH) as conn:
        await sql.createDB(conn)
        print("loading channels", flush=True)
        await loadChannels(conn)

        print("downloading content", flush=True)
        await discord.asyncFetchAllFiles(conn)
        print("done", flush=True)

    await asyncio.sleep(.2)  # sleep a bit to let connections wind down


if __name__ == "__main__":
    asyncio.run(main())
