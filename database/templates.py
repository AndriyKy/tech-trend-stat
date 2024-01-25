from datetime import UTC, datetime, timedelta
from os import environ, getenv
from typing import Any

from pydantic import BaseModel
from pymongo import ASCENDING, DESCENDING, ReplaceOne

from database.client import MongoClientSingleton


class Database:
    collection: str
    database: str
    indices: list[tuple[str, int]]
    cluster_host: str | None = None
    client: MongoClientSingleton | None = None

    def connect_database(self) -> None:
        self.client = MongoClientSingleton(
            cluster_host=self.cluster_host,
            database=self.database,
            is_test=True if environ["IS_TEST"].lower() == "true" else False,
            host=getenv("MONGODB_HOST"),
            port=int(getenv("MONGODB_PORT", 27017)),
            username=getenv("MONGODB_USERNAME"),
            password=getenv("MONGODB_PASSWORD"),
        )
        self.client[self.collection].create_index(self.indices, unique=True)

    def create_replacements(self, items: list[BaseModel]) -> list[ReplaceOne]:
        """Request replacements for `bulk_write` operation."""
        index_fields = [index[0] for index in self.indices]
        replacements = []
        for item in items:
            item = item.model_dump()
            indices = {index: item[index] for index in index_fields}
            replacements.append(ReplaceOne(indices, item, upsert=True))
        return replacements


class DatabaseVacancies(Database):
    database = "vacancy_statistics"
    collection = "vacancies"
    indices = [
        ("publication_date", DESCENDING),
        ("company_name", ASCENDING),
        ("years_of_experience", ASCENDING),
    ]

    def fetch_vacancies(
        self,
        from_datetime: timedelta,
        to_datetime: timedelta,
    ) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        vacancies = self.client[self.collection].aggregate(
            [
                {
                    "$match": {
                        "$and": [
                            {"publication_date": {"$lte": now - to_datetime}},
                            {
                                "publication_date": {
                                    "$gte": now - from_datetime
                                }
                            },
                        ]
                    }
                }
            ],
        )
        return [vacancy for vacancy in vacancies]


class DatabaseStatistics(Database):
    database = "vacancy_statistics"
    collection = "statistics"
    indices = [("technology_frequency", ASCENDING)]
