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
