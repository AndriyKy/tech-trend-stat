import re
from collections import Counter
from json import loads
from os.path import join as join_path

import spacy

STOPWORDS_DIR = join_path("techtrendanalysis", "stopwords")


class Wrangler:
    def __init__(self, text: str, extra_filters: set[str] = set()) -> None:
        self._text = text
        self._extra_filters = extra_filters
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
        return re.sub(pattern, " ", self._text)

    def calculate_frequency_distribution(
        self, limit_results: int = 20
    ) -> dict[str, int]:
        self._text = self._clean_text()

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

        return {
            lower_to_upper[noun_frequency[0]]: noun_frequency[1]
            for noun_frequency in proper_nouns_count.most_common(limit_results)
        }
