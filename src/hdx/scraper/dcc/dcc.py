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
    def __init__(self, configuration: Configuration, retriever: Retrieve, temp_dir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self.data = {}
        self.data_url = self._configuration["data_url"]

    def get_location_name(self, country_name: str) -> str:
        """Convert country name from data to HDX location name using
           HDX Python Country

        Args:
            country_name (str): Country name from data

        Returns:
            str: HDX location name
        """
        try:
            iso3 = Country.get_iso3_country_code_fuzzy(country_name)
            location_name = Country.get_country_name_from_iso3(iso3[0])
            return location_name
        except HDXError as e:
            logger.error(f"Error retrieving location for country '{country_name}': {e}")
            return "None"

    def _parse_table_content(self, table_content: str) -> Dict:
        """Parse table content into a dictionary

        Args:
            table_content (str): Content of the table

        Returns:
            Dict: Parsed country data
        """
        data = {}
        rows = [row.strip() for row in table_content.split("\n") if row.strip()]
        for row in rows:
            parts = [col.strip() for col in row.split("|") if col.strip()]
            if len(parts) < 3:
                logger.warning(f"Incomplete row skipped: {row}")
                continue

            country, walking, motorised = parts[:3]
            if country == "Tanzania_w_zanzibar":
                continue

            try:
                location_name = self.get_location_name(country)
                data[location_name] = {
                    "walking": walking,
                    "motorised": motorised,
                }
            except HDXError:
                logger.warning(f"Skipping country '{country}' due to location resolution error.")
        return data

    def get_country_data(self, text: str) -> Dict:
        """Extract TIF file urls for each country from markdown file

        Args:
            text (str): Markdown text

        Returns:
            Dict: Dict containing country name with two urls each
        """
        # Find the table of country data
        separator_pattern = re.compile(r"\|\:\-+\|(:?-+\|)+")
        separator_match = separator_pattern.search(text)
        if not separator_match:
            logger.error("Table separator line not found.")
            return {}

        table_start = separator_match.end()
        table_end = text.find("\n\n", table_start)
        table_content = (
            text[table_start:table_end].strip() if table_end != -1 else text[table_start:].strip()
        )

        return self._parse_table_content(table_content)

    def get_data(self) -> List:
        """Download markdown file, get country data

        Args:
            None

        Returns:
            List: Country names
        """
        try:
            # Download readme content
            content = self._retriever.download_text(self.data_url, filename="readme")

            # Extract country data
            self.data = self.get_country_data(content)

        except DownloadError:
            logger.error(f"Could not get data from {self.data_url}")
            return []

        return sorted(self.data)

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
            dataset_time_period = datetime.strptime(
                self._configuration["date_of_dataset"], "%B %Y"
            )
            dataset.set_time_period(dataset_time_period)
            dataset["methodology"] = "Other"
            dataset["methodology_other"] = dataset_info["methodology_other"]

            try:
                dataset.add_country_location(dataset_country_iso3)
            except HDXError:
                logger.error(f"Couldn't find country {dataset_country_iso3}, skipping")
                return []

            # Create resource
            resource_name = f"service_area_{country_name}_{data_type}.tif"
            resource_description = dataset_info["description"].replace("[country]", country_name)
            resource = {
                "name": resource_name,
                "description": resource_description,
                "url": url,
                "format": "GeoTIFF",
            }

            dataset.add_update_resources([resource])
            datasets.append(dataset)

        return datasets
