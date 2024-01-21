from typing import Any

from pymongo import ASCENDING, DESCENDING, ReplaceOne


class Database:
    collection: str
    database: str
    indices: list[tuple[str, int]] = []
    items: list[dict[str, Any]] = []
    cluster_host: str | None = None

    def create_replacements(self) -> list[ReplaceOne]:
        """Request replacements for `bulk_write` operation."""
        index_fields = [index[0] for index in self.indices]
        replacements = []
        for vacancy in self.items:
            indices = {index: vacancy[index] for index in index_fields}
            replacements.append(ReplaceOne(indices, vacancy, upsert=True))
        return replacements


class DatabaseVacancies(Database):
    database = "vacancy_statistics"
    collection = "vacancies"
    indices = [
        ("publication_date", DESCENDING),
        ("company_name", ASCENDING),
        ("years_of_experience", ASCENDING),
    ]
