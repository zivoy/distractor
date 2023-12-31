import asyncio
import hashlib
import json
import mimetypes
import os.path
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Union

import aiocsv as csv
import aiofiles
import aiohttp
import aiosqlite
from tqdm.auto import tqdm

import config
import sql

DISCORD_EPOCH = 1420070400000
URL_REGEX = re.compile(r"https://(?:cdn|media)\.discordapp\.(?:com|net)/attachments/"
                       r"(?P<channelId>\d+)/(?P<elementId>\d+)/(?P<name>[a-zA-Z0-9$\-_.+!*'(),]+)")
CONTENT_FOLDER = "content"

MAX_SIZE = config.settings.getint("MaxFileSizeMb") * 1024 * 1024

configFolder = os.path.join(config.settings["OutputLocation"], CONTENT_FOLDER)


def getDateFromSnowflake(snowflake: Union[str, int]) -> datetime:
    return datetime.fromtimestamp(((int(snowflake) >> 22) + DISCORD_EPOCH) / 1000)


async def loadChannel(con: aiosqlite.Connection, folder: str):
    jsonPath = os.path.join(folder, "channel.json")
    csvPath = os.path.join(folder, "messages.csv")

    async with aiofiles.open(jsonPath, encoding="utf8") as jsonFile:
        data = json.loads(await jsonFile.read())
    cid = data["id"]
    ctype = data["type"]
    name = data["name"] if "name" in data else None
    created = getDateFromSnowflake(cid)
    await con.execute(sql.INSERT_CHANNEL, (cid, ctype, name, created))
    # await con.commit()

    cur = await con.execute(sql.CHANNEL_LATEST_MESSAGES, (cid, 1))
    res = await cur.fetchone()
    latestTime = datetime(1, 1, 1, tzinfo=timezone.utc)
    if res is not None:
        latestId, latestTime = res
        latestTime = datetime.fromisoformat(latestTime)
        # print(f"latest stored message in {cid} is id {latestId} from {latestTime}")

    messages = list()
    async with aiofiles.open(csvPath, encoding="utf8") as jsonFile:
        async for row in csv.AsyncDictReader(jsonFile):
            time = datetime.fromisoformat(row["Timestamp"])
            if time <= latestTime:
                continue
            content = row["Contents"]
            attachments = row["Attachments"]
            messages.append((row["ID"], cid, content if content else None, attachments if attachments else None, time))
    await con.executemany(sql.INSERT_MESSAGE, messages)


async def asyncGetLinks(con: aiosqlite.Connection):
    res = await con.execute_fetchall(sql.HAS_CDN_CONTENT)
    data = defaultdict(set)
    for mId, text, att in res:
        for match in URL_REGEX.finditer(text):
            data[match.group()].add((mId, att))
    return dict(data)


def getfilename(mime_type: str, data: bytes) -> str:
    name = hashlib.blake2b(data, digest_size=10).hexdigest()
    extension = mimetypes.guess_extension(mime_type)
    # for some reason windows is missing some
    if not extension:
        mime_type = mime_type.lower()
        if mime_type == "image/webp":
            extension = ".webp"
        elif mime_type == "video/webm":
            extension = ".webm"
        elif mime_type == "application/x-7z-compressed":
            extension = ".7z"
        elif mime_type == "application/x-msdos-program":
            extension = ".exe"
        elif mime_type == "application/vnd.android.package-archive":
            extension = ".apk"
        elif mime_type == "application/java-archive":
            extension = ".jar"
        elif mime_type == "text/x-java":
            extension = ".java"
        elif mime_type == "text/x-sh":
            extension = ".sh"
        else:
            # print
            tqdm.write(f"unsure extension '{mime_type}' on '{name}'")
            extension = ""
    return f"{name}{extension}"


async def downloadFile(session: aiohttp.ClientSession, url: str):
    async with (session.get(url) as resp):
        if resp.status != 200:
            return None, "http error", resp.status, url
        contentType = resp.headers["Content-Type"]
        if ";" in contentType:
            contentType, *parameters = contentType.split(";")
            # parameters = ";".join(parameters)
            # charset = parameters.split("charset=")[1]  # ignoring specified charset, dont really need it
            # print(f"charset specified '{charset}' for {url}")

        size = None
        if "Content-Length" in resp.headers:  # not present in text file for some reason
            size = int(resp.headers["Content-Length"])
            if size > MAX_SIZE:
                return None, "too large", size, url

        content = await resp.content.read()
        if size is None:
            size = len(content)
        name = getfilename(contentType, content)
        fullpath = os.path.join(configFolder, name)
        if not os.path.exists(fullpath):
            async with aiofiles.open(fullpath, mode="wb") as f:
                await f.write(content)

        # have to return the url since async
        return url, contentType, os.path.join(CONTENT_FOLDER, name), size


async def asyncFetchAllFiles(conn: aiosqlite.Connection):
    files = await asyncGetLinks(conn)
    tasks = list()
    failed = list()
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(50)  # max concurrent requests // 25

        async def downloadSem(url):
            async with semaphore:
                return await downloadFile(session, url)

        for url in tqdm(files.keys(), desc="Enumerating local assets"):
            cur = await conn.execute(sql.URL_IN_CONTENT, (url,))
            res = await cur.fetchone()
            if not res:
                tasks.append(asyncio.create_task(downloadSem(url)))
            else:
                # make connections even if already have content
                for mId, attachment in files[url]:
                    await conn.execute(sql.INSERT_CONTENT_LINK, (mId, res[0], attachment))
        await conn.commit()

        for resp in tqdm(asyncio.as_completed(tasks), desc="Downloading assets", total=len(tasks)):
            res = await resp
            if res[0] is None:
                failed.append(res[1:])
                continue
            url, contentType, filepath, size = res

            match = URL_REGEX.fullmatch(url)
            assert (match is not None)
            groups = match.groupdict()

            filename = groups["name"]
            oChannelId = groups["channelId"]
            itemId = groups["elementId"]
            created = getDateFromSnowflake(itemId)
            connections = files[url]

            await conn.execute(sql.INSERT_CONTENT,
                               (itemId, oChannelId, filename, url, filepath, contentType, size, created))
            for mId, attachment in connections:
                await conn.execute(sql.INSERT_CONTENT_LINK, (mId, itemId, attachment))
            await conn.commit()

    if failed:
        print("failed to get:")
        for i in failed:
            print(f" {i[0]} ({i[1]}) - {i[2]}")
