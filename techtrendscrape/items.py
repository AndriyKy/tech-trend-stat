# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, NonNegativeInt


class VacancyItem(BaseModel):
    source: Literal["djinni"] = "djinni"
    category: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    company_type: str | None = Field(min_length=1)
    # TODO: Integrate smart search (nltk, TextBlob).
    # technologies: conlist(str, min_length=1)
    description: str
    years_of_experience: NonNegativeInt
    publication_date: datetime
    views: NonNegativeInt | None
    applications: NonNegativeInt | None
