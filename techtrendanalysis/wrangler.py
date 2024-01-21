import string
from pathlib import Path

import nltk
from nltk.corpus import stopwords
from nltk.probability import FreqDist
from nltk.tokenize import word_tokenize

CACHE_DIR = ".cache"


class Wrangle:
    def __init__(self) -> None:
        # Set the NLTK data directory to your custom directory.
        nltk.data.path.append(CACHE_DIR)

        if not Path(CACHE_DIR).exists():
            # Download stop words and tokenizer.
            nltk.download("stopwords", CACHE_DIR)
            nltk.download("punkt", CACHE_DIR)

    @staticmethod
    def calculate_frequency_distribution(
        text, limit_results: int = 20
    ) -> list[tuple[str, int]]:
        """Remove stop words and words that are not capitalized."""
        stop_words = set(stopwords.words("english"))
        words = word_tokenize(text)

        filtered_words = [
            word
            for word in words
            if word.lower() not in stop_words
            and word == word.capitalize()
            and word not in string.punctuation
        ]

        return FreqDist(filtered_words).most_common(limit_results)
