from typing import Any, Self
from urllib.parse import quote

from pymongo import MongoClient
from pymongo.collection import Collection


class MongoClientSingleton:
    """Connect to a local MongoDB if `is_test` is `True`.
    Otherwise, connect to a Mongo cloud.

    NOTE: Don't forget to close the connection!
    """

    _instance = None

    def __new__(
        cls,
        *,
        database: str,
        is_test: bool = True,
        cluster_host: str | None = None,
        **kwargs: Any,
    ) -> Self:
        if not cls._instance:
            if not is_test:
                if (
                    not (username := kwargs.get("username"))
                    or not (password := kwargs.get("password"))
                    or not cluster_host
                ):
                    raise ValueError(
                        "`username`, `password` and `cluster` "
                        "are required in a production environment."
                    )
                client_kwargs = dict(
                    host=f"mongodb+srv://{username}:{quote(password)}"
                    f"@{cluster_host}.mongodb.net/?retryWrites=true&w=majority"
                )
            else:
                client_kwargs = kwargs
            cls._database = database
            cls._client = MongoClient(**client_kwargs)
            cls._instance = super().__new__(cls)
        return cls._instance

    def __getitem__(self, collection_name: str) -> Collection:
        return self._client[self._database][collection_name]

    @property
    def close(self) -> None:
        self._client.close()