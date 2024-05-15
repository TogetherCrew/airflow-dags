from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class ModulesBase:
    def __init__(self) -> None:
        pass

    def query(self, platform: str, **kwargs) -> list[dict]:
        """
        query the modules database for to get platforms' metadata

        Parameters
        -----------
        platform : str
            the platform to choose
            it can be `github`, `discourse`, `discord` or etc
        **kwargs : dict
            projection : dict[str, int]
                feature projection on query

        Returns
        ---------
        modules_docs : list[dict]
            all the module documents that have the `platform` within them
        """
        client = MongoSingleton.get_instance().client
        projection = kwargs.get("projection", {})

        cursor = client["Core"]["modules"].find(
            {
                "options.platforms.name": platform,
                "name": "hivemind",
            },
            projection,
        )
        modules_docs = list(cursor)
        return modules_docs

    def get_platform_community_ids(self, platform_name: str) -> list[str]:
        """
        get all community ids that a platform has

        Parameters
        ------------
        platform_name : str
            the platform having community id and available for hivemind module

        Returns
        --------
        community_ids : list[str]
            id of communities that has discord platform and hivemind module enabled

        """
        modules = self.query(platform=platform_name, projection={"community"})
        community_ids = list(map(lambda x: str(x["community"]), modules))

        return community_ids

    def get_token(self, platform_id: ObjectId, token_type: str) -> str:
        """
        get a specific type of token for a platform
        This method is called when we needed a token for modules to extract its data

        Parameters
        ------------
        platform_id : ObjectId
            the platform id that we want their token
        token_type : str
            the type of token. i.e. `google_refresh`

        Returns
        --------
        token : str
            the token that was required for module's ETL process
        """
        client = MongoSingleton.get_instance().client

        user = self._get_platform_userid(platform_id)
        token_doc = client["Core"]["tokens"].find_one(
            {
                "user": user,
                "type": token_type,
            },
            {
                "token": 1,
            },
            sort=[("createdAt", 1)],
        )
        if token_doc is None:
            raise ValueError(
                f"No Token for the given user {user} "
                "in tokens collection of the Core database!"
            )
        token = token_doc["token"]
        return token

    def _get_platform_userid(self, platform_id: ObjectId) -> str:
        """
        get the userid that belongs to a platform

        Parameters
        -----------
        platform_id : bson.ObjectId
            the platform id we need their owner user id

        Returns
        ---------
        user_id : str
            the user id that the platform belongs to
        """
        client = MongoSingleton.get_instance().get_client()

        platform = client["Core"]["platforms"].find_one(
            {
                "_id": platform_id,
            },
            {
                "metadata.userId": 1,
            },
        )
        if platform is None:
            raise ValueError(f"No platform available given platform id: {platform_id}")

        user_id = platform["metadata"]["userId"]
        return ObjectId(user_id)
