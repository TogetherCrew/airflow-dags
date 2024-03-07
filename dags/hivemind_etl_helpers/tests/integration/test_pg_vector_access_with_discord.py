import unittest
from datetime import datetime, timedelta

import psycopg2
from bson import ObjectId
from hivemind_etl_helpers.src.db.discord.discord_raw_message_to_document import (
    discord_raw_to_docuemnts,
)
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton
from llama_index.indices.vector_store import VectorStoreIndex
from tc_hivemind_backend.db.credentials import load_postgres_credentials
from tc_hivemind_backend.db.pg_db_utils import setup_db
from tc_hivemind_backend.pg_vector_access import PGVectorAccess


class TestPGVectorAccess(unittest.TestCase):
    def setUpDB(self, dbname: str, table_name: str):
        creds = load_postgres_credentials()
        self.community_id = "12345698765dfgh"
        latest_date_query = f"""
            SELECT (metadata_->> 'date')::timestamp AS latest_date
            FROM data_{table_name}
            ORDER BY (metadata_->>'date')::timestamp DESC
            LIMIT 1;
        """
        setup_db(
            community_id=self.community_id,
            dbname=dbname,
            latest_date_query=latest_date_query,
        )
        self.postgres_conn = psycopg2.connect(
            dbname=dbname,
            user=creds["user"],
            password=creds["password"],
            host=creds["host"],
            port=creds["port"],
        )

    def setup_mongo_information(
        self,
        channels: list[str],
        create_modules: bool = True,
        create_platform: bool = True,
        guild_id: str = "1234",
    ):
        client = MongoSingleton.get_instance().client

        community_id = ObjectId("9f59dd4f38f3474accdc8f24")
        platform_id = ObjectId("063a2a74282db2c00fbc2428")

        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")

        if create_modules:
            data = {
                "name": "hivemind",
                "communityId": community_id,
                "options": {
                    "platforms": [
                        {
                            "platformId": platform_id,
                            "fromDate": datetime(2023, 1, 1),
                            "options": {
                                "channels": channels,
                                "roles": ["role_id"],
                                "users": ["user_id"],
                            },
                        }
                    ]
                },
            }
            client["Core"]["modules"].insert_one(data)

        if create_platform:
            client["Core"]["platforms"].insert_one(
                {
                    "_id": platform_id,
                    "name": "discord",
                    "metadata": {
                        "action": {
                            "INT_THR": 1,
                            "UW_DEG_THR": 1,
                            "PAUSED_T_THR": 1,
                            "CON_T_THR": 4,
                            "CON_O_THR": 3,
                            "EDGE_STR_THR": 5,
                            "UW_THR_DEG_THR": 5,
                            "VITAL_T_THR": 4,
                            "VITAL_O_THR": 3,
                            "STILL_T_THR": 2,
                            "STILL_O_THR": 2,
                            "DROP_H_THR": 2,
                            "DROP_I_THR": 1,
                        },
                        "window": {"period_size": 7, "step_size": 1},
                        "id": guild_id,
                        "isInProgress": False,
                        "period": datetime.now() - timedelta(days=35),
                        "icon": "some_icon_hash",
                        "selectedChannels": channels,
                        "name": "GuildName",
                    },
                    "community": community_id,
                    "disconnectedAt": None,
                    "connectedAt": datetime.now(),
                    "createdAt": datetime.now(),
                    "updatedAt": datetime.now(),
                }
            )

    def _create_and_save_doc(self, table: str, guild_id: str, dbname: str):
        client = MongoSingleton.get_instance().client
        dbname = f"guild_{guild_id}"
        channels = ["111111", "22222"]
        self.setup_mongo_information(
            channels=channels,
            guild_id=guild_id,
        )

        client[guild_id].drop_collection("rawinfos")
        client[guild_id].drop_collection("guildmembers")

        client[guild_id]["rawinfos"].insert_one(
            {
                "type": 0,
                "author": "111",
                "content": "test_message1",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(2023, 5, 1),
                "messageId": "100000000",
                "channelId": channels[0],
                "channelName": "channel1",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
        )
        client[guild_id]["guildmembers"].insert_one(
            {
                "discordId": "111",
                "username": "sample_user_0",
                "globalName": "SampleUserGlobalName",
                "nickname": None,
                "roles": [],
                "joinedAt": datetime.now(),
                "avatar": "sample_avatar$#WEAasdkpmdpa3256",
                "isBot": False,
                "discriminator": "0",
            }
        )

        documents = discord_raw_to_docuemnts(guild_id=guild_id)
        self.setUpDB(dbname=dbname, table_name=table)
        cursor = self.postgres_conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS data_{table};")
        self.postgres_conn.commit()
        pg_vector = PGVectorAccess(table_name=table, dbname=dbname, testing=True)

        # Save the documents
        pg_vector.save_documents(self.community_id, documents, verbose=True)
        return documents

    def test_save_documents(self):
        table = "discord"
        guild_id = "1234"
        dbname = f"guild_{guild_id}"

        documents = self._create_and_save_doc(table, guild_id, dbname)
        cursor = self.postgres_conn.cursor()

        cursor.execute(f"SELECT text, node_id, metadata_ FROM data_{table};")

        data = cursor.fetchall()

        self.assertEqual(len(data), 1)

        # text, node_id, metadata
        text, _, metadata = data[0]

        # nickname was `None`, so it wasn't included in metadata
        self.assertEqual(text, "test_message1")
        self.assertEqual(metadata["channel"], documents[0].metadata["channel"])
        self.assertEqual(
            metadata["author_username"], documents[0].metadata["author_username"]
        )
        self.assertEqual(
            metadata["author_global_name"], documents[0].metadata["author_global_name"]
        )
        self.assertEqual(metadata["date"], documents[0].metadata["date"])
        self.assertNotIn("author_nickname", metadata)

        cursor.close()

    def test_load_index(self):
        table = "discord"
        guild_id = "1234"
        dbname = f"guild_{guild_id}"
        _ = self._create_and_save_doc(table, guild_id, dbname)

        pg_vector = PGVectorAccess(table_name=table, dbname=dbname, testing=True)
        index = pg_vector.load_index()

        self.assertIsInstance(index, VectorStoreIndex)

        cursor = self.postgres_conn.cursor()
        cursor.close()
