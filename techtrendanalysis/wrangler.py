import csv
import re
from collections import Counter
from datetime import UTC, datetime, timedelta
from json import loads
from os.path import join as join_path
from pathlib import Path
from typing import Any

import spacy
from pymongo.results import BulkWriteResult

from database import DatabaseStatistics, DatabaseVacancies, Statistics

STOPWORDS_DIR = join_path("techtrendanalysis", "stopwords")


class Wrangler(DatabaseVacancies):
    def __init__(
        self,
        text: str | None,
        category: str | None,
        extra_filters: set[str] = set(),
    ) -> None:
        self._text = text
        self._category = category
        self._extra_filters = extra_filters
        self._from_datetime: timedelta = timedelta(days=0)
        self._to_datetime: timedelta = timedelta(days=0)

        if (not self._text and not self._category) or (
            self._text and self._category
        ):
            raise ValueError("Either `text` or `category` is required!")

        with open(
            join_path(STOPWORDS_DIR, "ukrainian-stopwords.json")
        ) as ukr_stopwords, open(
            join_path(STOPWORDS_DIR, "common-words.json")
        ) as common_words:
            self.stopwords = set(
                loads(ukr_stopwords.read()) + loads(common_words.read())
            )

    def _clean_text(self) -> str:
        to_filter = {"<br>", "<b>", "</b>", "• ", "- "}.union(
            self._extra_filters
        )
        pattern = re.compile(rf"{'|'.join(to_filter)}", flags=re.IGNORECASE)
        self._text = re.sub(pattern, " ", self._text)

    def extract_text_from_vacancies(
        self,
        from_datetime: timedelta = timedelta(days=30),
        to_datetime: timedelta = timedelta(days=0),
    ) -> None:
        self._from_datetime = from_datetime
        self._to_datetime = to_datetime
        self.connect_database()
        vacancies = self.fetch_vacancies(
            self._category, from_datetime, to_datetime
        )
        self._text = " ".join(
            [vacancy["description"] for vacancy in vacancies]
        )
        self.client.close

    def calculate_frequency_distribution(
        self, limit_results: int = 20
    ) -> Statistics:
        if not self._text:
            self.extract_text_from_vacancies()
        self._clean_text()

        nlp = spacy.load("en_core_web_sm")  # Load the spaCy model.
        doc = nlp(self._text)  # Process the text with spaCy.

        # Unicode ranges for English letters.
        eng_uppercase, eng_lowercase = range(65, 90), range(97, 122)

        proper_nouns, lower_to_upper = [], {}
        for token in doc:
            if (
                token.pos_ == "PROPN"  # IT techs are mostly proper nouns.
                and (token_text := token.text) not in self.stopwords
                and (
                    ord(token_text[0]) in eng_uppercase
                    or ord(token_text[0]) in eng_lowercase
                )
            ):
                proper_nouns.append(token_text.lower())
                lower_to_upper[token_text.lower()] = token_text

        proper_nouns_count = Counter(proper_nouns)
        now = datetime.now(UTC)
        return Statistics(
            from_datetime=now - self._from_datetime,
            to_datetime=now - self._to_datetime,
            technology_frequency={
                lower_to_upper[noun_frequency[0]]: noun_frequency[1]
                for noun_frequency in proper_nouns_count.most_common(
                    limit_results
                )
            },
        )

    @staticmethod
    def save_statistics(
        statistics: Statistics, *, to_db: bool = True
    ) -> BulkWriteResult | Any:
        if to_db:
            database = DatabaseStatistics()
            database.connect_database()
            bulk_write_result = database.client[
                database.collection
            ].bulk_write(database.create_replacements([statistics]))
            database.client.close
            return bulk_write_result

        file = Path(f"{DatabaseStatistics.collection}.csv")
        file_exists = file.exists()
        fieldnames = list(statistics.model_json_schema()["properties"].keys())
        with open(file, "a") as fp:
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            writer.writeheader() if not file_exists else None
            return writer.writerow(statistics.model_dump(mode="json"))
