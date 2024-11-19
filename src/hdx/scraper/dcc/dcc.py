#!/usr/bin/python
"""dcc scraper"""

import logging
import os
import re
from datetime import datetime
from typing import Dict, List

import rasterio
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dictandlist import dict_of_lists_add
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

    def compress_tif(self, input_path, output_path, compression_level=6):
        """Compress a TIF file using Rasterio deflate compression

        Args:
            input_path (str): Path to input TIF file
            output_path (str): Path where compressed TIF will be saved
            compression_level (int): Compression level (1-9),
            higher = smaller file but slower compression. Defaults to 6

        Returns:
            tuple: Original and new file sizes in MB
        """
        with rasterio.open(input_path) as src:
            # Compression settings
            profile = src.profile.copy()
            profile.update(
                {
                    "compress": "deflate",
                    "zlevel": compression_level,
                    "predictor": 2,
                }
            )

            # Create output file
            with rasterio.open(output_path, "w", **profile) as dst:
                for i in range(1, src.count + 1):
                    dst.write(src.read(i), i)

        # Get file sizes in MB
        original_size = os.path.getsize(input_path) / (1024 * 1024)
        compressed_size = os.path.getsize(output_path) / (1024 * 1024)

        return original_size, compressed_size

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
        """Download markdown file, get country data,
           download tif files, run compression

        Args:
            None

        Returns:
            List: Dataset names
        """
        try:
            # Download readme content
            content = self._retriever.download_text(
                self.data_url, filename="readme"
            )

            # Extract country data
            country_data = self.get_country_data(content)

            # Download files for each country
            count = 0  # Just do one for testing
            for country, files in country_data.items():
                if count < 1:
                    for data_type in ["walking", "motorised"]:
                        country_object = {}
                        file_url = files[data_type]
                        filename = file_url.split("/")[-1]
                        dataset_name = f"service_area_{country}_{data_type}"

                        try:
                            filepath = self._retriever.download_file(
                                file_url, filename=filename
                            )

                            # Create output filename
                            output_filename = (
                                f"{self._retriever.saved_dir}/{filename[:-4]}"
                            )
                            output_filepath = (
                                f"{output_filename}_compressed.tif"
                            )

                            orig_size, new_size = self.compress_tif(
                                filepath, output_filepath
                            )
                            logger.info(
                                f"Successfully compressed {country}: "
                                f"{filename} from {orig_size} to {new_size}"
                            )

                        except DownloadError:
                            logger.error(
                                f"Could not download {filename} for {country}"
                            )
                            continue
                        except Exception as e:
                            logger.error(f"""Error processing {filename}
                                for {country}: {str(e)}""")

                        country_object["filepath"] = output_filepath
                        country_object["dataset_name"] = dataset_name
                        country_object["data_type"] = data_type
                        dict_of_lists_add(self.data, country, country_object)
                count = count + 1

            return [country for country in sorted(self.data)]

        except DownloadError:
            logger.error(f"Could not get data from {self.data_url}")
            return {}

    def generate_dataset(self, country_name: str) -> Dataset:
        country_data = self.data[country_name]
        dataset_type = country_data[0]["data_type"]
        dataset_info = self._configuration[dataset_type]
        dataset_title = f"{country_name} {dataset_info['title']}"
        slugified_name = slugify(dataset_title)

        # Dataset info
        dataset = Dataset(
            {
                "name": slugified_name,
                "notes": dataset_info["notes"],
                "title": dataset_title,
            }
        )

        dataset.add_tags(self._configuration["tags"])
        dataset.set_expected_update_frequency(
            self._configuration["data_update_frequency"]
        )
        dataset_country_iso3 = Country.get_iso3_country_code(country_name)

        dataset_time_period = datetime.strptime(
            self._configuration["date_of_dataset"], "%B %Y"
        )
        dataset.set_time_period(dataset_time_period)
        dataset.set_subnational(False)
        try:
            dataset.add_country_location(dataset_country_iso3)
        except HDXError:
            logger.error(
                f"Couldn't find country {dataset_country_iso3}, skipping"
            )
            return

        resource_name = country_data[0]["dataset_name"]
        resource_description = dataset_info["description"].replace(
            "[country]", country_name
        )
        resource = Resource(
            {
                "name": resource_name,
                "id": slugify(resource_name),
                "format": "GeoTIFF",
                "description": resource_description,
            }
        )

        resource.set_file_to_upload(self.data[country_name][0]["filepath"])
        dataset.add_update_resources([resource])

        return dataset
