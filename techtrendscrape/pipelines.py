# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from typing import Self

from scrapy.crawler import Crawler

from database import DatabaseVacancies, MongoClientSingleton
from techtrendscrape.items import VacancyItem
from techtrendscrape.spiders.djinni import DjinniSpider


class MongoPipeline(DatabaseVacancies):
    def __init__(
        self,
        is_test: bool,
        host: str,
        port: int,
        username: str | None,
        password: str | None,
    ) -> None:
        self.db = MongoClientSingleton(
            cluster_host=self.cluster_host,
            database=self.database,
            is_test=is_test,
            host=host,
            port=port,
            username=username,
            password=password,
        )

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        return cls(
            is_test=crawler.settings.getbool("IS_TEST"),
            host=crawler.settings.get("MONGODB_HOST"),
            port=crawler.settings.getint("MONGODB_PORT"),
            username=crawler.settings.get("MONGODB_USERNAME"),
            password=crawler.settings.get("MONGODB_PASSWORD"),
        )

    def close_spider(self, spider: DjinniSpider) -> None:
        self.db[self.collection].create_index(self.indices, unique=True)
        self.db[self.collection].bulk_write(self.create_replacements())
        self.db.close

    def process_item(
        self, item: VacancyItem, spider: DjinniSpider
    ) -> VacancyItem:
        self.items.append(item.model_dump())
        return item
