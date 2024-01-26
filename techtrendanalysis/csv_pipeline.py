import csv

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from database import Statistics
from techtrendanalysis.wrangler import Wrangler


class CrawlTrends:
    def __init__(self, category: str = "Python") -> None:
        self._category = category
        self._settings = get_project_settings()
        # Save the results to a CSV file instead of MongoDB.
        self._settings.update(
            {
                "ITEM_PIPELINES": {
                    "techtrendscrape.pipelines.CSVPipeline": 300,
                }
            }
        )

    def start(self) -> None:
        process = CrawlerProcess(self._settings)
        process.crawl("djinni", categories=self._category)
        process.start()

    def extract_statistics(self) -> Statistics:
        with open(f"{Wrangler.collection}.csv") as csv_file:
            reader = csv.DictReader(csv_file)
            description = " ".join([item["description"] for item in reader])
        wrangler = Wrangler(description, self._category)
        return wrangler.calculate_frequency_distribution()
