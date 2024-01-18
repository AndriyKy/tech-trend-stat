# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from typing import Any, Self
from urllib.parse import quote

from pymongo import MongoClient
from scrapy.crawler import Crawler

from techtrendscrape.items import VacancyItem
from techtrendscrape.spiders.djinni import DjinniSpider


class MongoPipeline:
    database = "vacancies"
    collection_name = "djinni"

    def __init__(
        self,
        is_test: bool,
        host: str,
        port: int,
        username: str | None,
        password: str | None,
    ) -> None:
        self.is_test = is_test
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        return cls(
            is_test=crawler.settings.getbool("IS_TEST", True),
            host=crawler.settings.get("MONGODB_HOST" "localhost"),
            port=crawler.settings.getint("MONGODB_PORT", 27017),
            username=crawler.settings.get("MONGODB_USERNAME"),
            password=crawler.settings.get("MONGODB_PASSWORD"),
        )

    def connect_database(self) -> None:
        if not self.is_test:
            self.client = MongoClient(
                f"mongodb+srv://{self.username}:{quote(self.password)}"
                f"@{self.database}.clnome3.mongodb.net/"
                "?retryWrites=true&w=majority"
            )
        else:
            self.client = MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )
        self.db = self.client[self.database]

    def open_spider(self, spider: DjinniSpider) -> None:
        self.items: list[dict[str, Any]] = []
        self.connect_database()

    def close_spider(self, spider: DjinniSpider) -> None:
        self.db[self.collection_name].insert_many(self.items)
        self.client.close()

    def process_item(
        self, item: VacancyItem, spider: DjinniSpider
    ) -> VacancyItem:
        self.items.append(item.model_dump())
        return item
