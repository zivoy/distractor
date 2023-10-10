import asyncio
import json
import os

import tqdm

import config
import discord
import sql


def loadChannelFromFolder(conn, folder):
    with open(os.path.join(folder, "channel.json"), encoding="utf8") as jsonFile:
        with open(os.path.join(folder, "messages.csv"), encoding="utf8") as csvFile:
            discord.loadChannel(conn, jsonFile, csvFile)


def loadChannels(conn):
    containingFolder = config.settings["SourceFolder"]
    channels = list(os.scandir(os.path.join(containingFolder, "messages")))
    for channel in tqdm.tqdm(channels, desc="Loading channels"):
        if channel.is_file():
            continue
        # print("loading", channel.name)
        loadChannelFromFolder(conn, channel.path)

    # update channel names
    curr = conn.cursor()
    with open(os.path.join(containingFolder, "messages", "index.json")) as f:
        data = json.load(f)
        for k, v in data.items():
            curr.execute(sql.UPDATE_CHANNEL_NAME, (v, k))
    conn.commit()


def main():
    if not os.path.exists(sql.path):
        print("db doesnt exist, creating")
        con = sql.getConnection()
        sql.createDB(con)
        print("loading channels")
        loadChannels(con)
        con.close()
    # todo make better loading for where stuff was left off

    print("downloading content")
    asyncio.run(discord.asyncFetchAllFiles())
    print("done")


if __name__ == "__main__":
    main()
