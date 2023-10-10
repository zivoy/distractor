import os.path
import sqlite3

import aiosqlite

import config

DBFILE = "data.db"
path = os.path.join(config.settings["OutputLocation"], DBFILE)
DOWNLOAD_LIMIT = config.settings.getint("DownloadLimit")


def getConnection():
    return sqlite3.connect(path)


async def getConnectionAsync():
    return await aiosqlite.connect(path)


INSERT_CHANNEL = """
INSERT OR IGNORE INTO `channels`
(`id`, `type`, `name`, `created`) VALUES
(?, ?, ?, ?);
"""
INSERT_MESSAGE = """
INSERT OR IGNORE INTO `messages` 
(`id`, `channelId`, `content`, `attachments`, `created`) VALUES 
(?, ?, ?, ?, ?);
"""
INSERT_CONTENT = """
INSERT OR IGNORE INTO `content` 
(`id`, `originalChannelId`, `filename`, `url`, `filepath`, `contentType`, `size`, `created`) VALUES 
(?, ?, ?, ?, ?, ?, ?, ?);
"""
INSERT_CONTENT_LINK = """
INSERT OR IGNORE INTO `messageContents` 
(`messageId`, `contentId`, `attachment`) VALUES 
(?, ?, ?);
"""
UPDATE_CHANNEL_NAME = """
UPDATE OR IGNORE `channels` 
SET `name` = ?
WHERE `name` is NULL and id = ?;
"""
URL_IN_CONTENT = """
SELECT id FROM `content` 
WHERE url = ?;
"""
CHANNEL_LATEST_MESSAGES = """
SELECT id, created FROM messages 
WHERE channelId = ?
ORDER BY created DESC
LIMIT ?;
"""

# HAS_CDN_CONTENT = """
# SELECT * FROM messages
#      WHERE attachments IS NOT NULL OR
#            content LIKE '%cdn.discordapp.com%' OR
#            content LIKE '%media.discordapp.net%';
# """
HAS_CDN_CONTENT = f"""
WITH cont AS (SELECT id, content AS text, FALSE AS attachment
              FROM messages
              WHERE content LIKE '%cdn.discordapp.com%'
                 OR content LIKE '%media.discordapp.net%'),
     attch AS (SELECT id, attachments AS text, TRUE AS attachment
               FROM messages
               WHERE attachments IS NOT NULL)

SELECT *
FROM cont
UNION ALL
SELECT *
FROM attch
{'' if DOWNLOAD_LIMIT < 0 else f'limit {DOWNLOAD_LIMIT}'};
"""


def createDB(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS `channels` (
        `id`        INT         NOT NULL,
        `type`      INT         NOT NULL,
        --`guildId`    INT,
        `name`      TEXT,
        `created`   TIMESTAMP   NOT NULL,
        
        PRIMARY KEY (`id`)
    ) WITHOUT ROWID;
    """)
    # list of messages
    cur.execute("""
    CREATE TABLE IF NOT EXISTS `messages` (
        `id`            INT         NOT NULL,
        `channelId`     INT         NOT NULL,
        `content`       TEXT,
        `attachments`   TEXT,
        `created`       TIMESTAMP   NOT NULL,
        
        PRIMARY KEY (`id`),
        FOREIGN KEY(`channelId`) REFERENCES channels(`id`)
    ) WITHOUT ROWID;
    """)
    # list of images and videos, can post to same post
    cur.execute("""
    CREATE TABLE IF NOT EXISTS `content` (
        `id`                INT         NOT NULL,
        `originalChannelId` INT         NOT NULL,
        `filename`          TEXT        NOT NULL,
        `url`               TEXT        NOT NULL,
        `filepath`          TEXT        NOT NULL,
        `contentType`       TEXT        NOT NULL,
        `size`              INT         NOT NULL,
        `created`           TIMESTAMP   NOT NULL,
        
        PRIMARY KEY (`id`)
    ) WITHOUT ROWID;
    """)
    cur.execute("""
     CREATE TABLE IF NOT EXISTS `messageContents` (
         --`id`           INT,
         `messageId`    INT     NOT NULL,
         `contentId`    INT     NOT NULL,
         `attachment`   BOOLEAN NOT NULL,

         --PRIMARY KEY (`id` ASC),
         FOREIGN KEY(`messageId`) REFERENCES messages(`id`),
         FOREIGN KEY(`contentId`) REFERENCES content(`id`),
         UNIQUE(messageId, contentId)
     );
     """)

    con.commit()
