#!/usr/bin/python
"""dcc scraper"""

import logging
import re
from datetime import datetime
from typing import Dict, List

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class DCC:
    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self.data = {}
        self.data_url = self._configuration["data_url"]

    def get_location_name(self, country_name) -> str:
        """Convert country name from data to HDX location name using
           HDX Python Country

        Args:
            country_name (str): Country name from data

        Returns:
            str: HDX location name
        """
        iso3 = Country.get_iso3_country_code_fuzzy(country_name)
        location_name = Country.get_country_name_from_iso3(iso3[0])
        return location_name

    def get_country_data(self, text: str) -> Dict:
        """Extract TIF file urls for each country from markdown file

        Args:
            text (str): Markdown text

        Returns:
            Dict: Dict containing country name with two urls each
        """
        result = {}

        # Find the table of country data
        separator_pattern = r"\|\:\-+\|(:?-+\|)+"
        separator_match = re.search(separator_pattern, text)
        if separator_match:
            # Country data starts after the separator line and
            # ends at next double newline
            table_start = separator_match.end()
            table_end = text.find("\n\n", table_start)
            table_content = text[table_start:table_end].strip()

            # Split content by pipe character and strip any
            # whitespace and empty strings
            parts = [part.strip() for part in table_content.split("|")]
            parts = [part for part in parts if part]

            # Process parts to extract country, walking, motorised
            for i in range(0, len(parts), 3):
                if i + 2 < len(parts):
                    country = parts[i]

                    # Skip 'Tanzania_w_zanzibar'
                    if country == "Tanzania_w_zanzibar":
                        continue

                    location_name = self.get_location_name(country)
                    walking = parts[i + 1]
                    motorised = parts[i + 2]
                    result[location_name] = {
                        "walking": walking,
                        "motorised": motorised,
                    }
        else:
            print("Table separator line not found.")

        return result

    def get_data(self) -> List:
        """Download markdown file, get country data

        Args:
            None

        Returns:
            List: country names
        """
        try:
            # Download readme content
            content = self._retriever.download_text(
                self.data_url, filename="readme"
            )

            # Extract country data
            self.data = self.get_country_data(content)

        except DownloadError:
            logger.error(f"Could not get data from {self.data_url}")
            return {}

        return [country for country in sorted(self.data)]

    def generate_dataset(self, country_name: str) -> List[Dataset]:
        datasets = []

        # Get country data
        country_data = self.data[country_name]

        # Iterate through file types for each country
        for data_type, url in country_data.items():
            dataset_info = self._configuration[data_type]
            dataset_title = f"{country_name} {dataset_info['title']}"
            slugified_name = slugify(dataset_title)

            # Create dataset
            dataset = Dataset(
                {
                    "name": slugified_name,
                    "notes": dataset_info["notes"],
                    "title": dataset_title,
                }
            )

            # Add dataset info
            dataset.add_tags(self._configuration["tags"])
            dataset_country_iso3 = Country.get_iso3_country_code(country_name)
            dataset.set_expected_update_frequency(
                self._configuration["data_update_frequency"]
            )
            dataset_time_period = datetime.strptime(
                self._configuration["date_of_dataset"], "%B %Y"
            )
            dataset.set_time_period(dataset_time_period)
            dataset.set_subnational(False)
            dataset["methodology"] = "Other"
            dataset["methodology_other"] = dataset_info["methodology_other"]

            try:
                dataset.add_country_location(dataset_country_iso3)
            except HDXError:
                logger.error(
                    f"Couldn't find country {dataset_country_iso3}, skipping"
                )
                return

            # Create resource
            resource_name = f"service_area_{country_name}_{data_type}.tif"
            resource_description = dataset_info["description"].replace(
                "[country]", country_name
            )
            resource = {
                "name": resource_name,
                "description": resource_description,
                "url": url,
                "format": "GeoTIFF",
            }

            dataset.add_update_resources([resource])
            datasets.append(dataset)

        return datasets
