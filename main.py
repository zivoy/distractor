import asyncio
import json
import os
import shutil

import aiosqlite
from tqdm.auto import tqdm

import config
import discord
import sql
import zipfile

async def loadChannels(conn: aiosqlite.Connection, containingFolder=None):
    if containingFolder is None:
        containingFolder = config.settings["SourceFolder"]

    print("loading channels", flush=True)
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


# going for the easy route
async def loadZipChannels(conn: aiosqlite.Connection):
    tempPath = os.path.join(config.settings["OutputLocation"], "temp")
    print("extracting messages", flush=True)
    with zipfile.ZipFile(config.settings["SourceZip"]) as z:
        for name in z.namelist():
            if name.startswith("messages/"):
                z.extract(name, tempPath)

    await loadChannels(conn, tempPath)

    shutil.rmtree(tempPath)


async def main():
    async with aiosqlite.connect(sql.DB_PATH) as conn:
        await sql.createDB(conn)

        if "SourceZip" in config.settings:
            await loadZipChannels(conn)
        elif "SourceFolder" in config.settings:
            await loadChannels(conn)
        else:
            print("no source specified", flush=True)
            return

        print("downloading content", flush=True)
        if not os.path.exists(discord.configFolder):
            os.mkdir(discord.configFolder)
        await discord.asyncFetchAllFiles(conn)
        print("done", flush=True)

    await asyncio.sleep(.2)  # sleep a bit to let connections wind down


if __name__ == "__main__":
    asyncio.run(main())
