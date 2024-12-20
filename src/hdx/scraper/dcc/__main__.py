#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from src.hdx.scraper.dcc.dcc import DCC

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-dcc"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: dcc"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=temp_dir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=temp_dir,
                save=save,
                use_saved=use_saved,
            )
            configuration = Configuration.read()
            #
            # Steps to generate dataset
            #
            dcc = DCC(configuration, retriever, temp_dir)

            countries = dcc.get_data()

            for country in countries:
                try:
                    datasets = dcc.generate_dataset(country)

                    for dataset in datasets:
                        dataset.update_from_yaml(
                            path=join(
                                dirname(__file__),
                                "config",
                                "hdx_dataset_static.yaml",
                            )
                        )

                        # Debug code
                        # logger.info(f"Dataset details for {country}:")
                        # logger.info(f"Name: {dataset.get('name')}")
                        # logger.info(f"Title: {dataset.get('title')}")
                        # logger.info(f"Resources: {dataset.get_resources()}")

                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            match_resource_order=False,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                            batch=info["batch"],
                        )
                except HDXError as e:
                    logger.exception(f"HDX error for {country}", exc_info=e)
                    continue


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(dirname(__file__), "config", "project_configuration.yaml"),
    )
