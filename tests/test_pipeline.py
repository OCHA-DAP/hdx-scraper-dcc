from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.dcc.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestDcc",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)

                countries = pipeline.get_data()
                for country in countries[:1]:
                    datasets = pipeline.generate_dataset(country)
                    for dataset in datasets:
                        dataset.update_from_yaml(
                            path=join(config_dir, "hdx_dataset_static.yaml")
                        )

                        assert dataset == {
                            "caveats": "The United Nations Office for the Coordination of Humanitarian Affairs "
                            "(OCHA) Common Operational Datasets (COD) Administration Level 0 boundary polygons "
                            "were used in instances where geoBoundaries simplified polygons were not available.\n"
                            "\n"
                            "Where countries were not included in the health facility data published by Maina et "
                            "al. (2019) we used data from the Global Health sites Mapping Project published on "
                            "Humanitarian Data Exchange (this included: Egypt, Libya, Tunisia, Algeria, Morocco).\n"
                            "\n"
                            "For each country we removed health sites that were unlikely to offer child focused "
                            "services and vaccinations. Facilities that were removed included: pharmacy, dentist, "
                            "veterinary, café/pharmacy, social facility.\n"
                            "\n"
                            "The accuracy of the road data sets has not been validated,\n"
                            "\n"
                            "[Maina et al. (2019)](https://www.nature.com/articles/s41597-019-0142-2)\n",
                            "data_update_frequency": 365,
                            "dataset_date": "[2024-08-01T00:00:00 TO 2024-08-01T00:00:00]",
                            "dataset_source": "Data for Children Collaborative",
                            "groups": [{"name": "dza"}],
                            "license_id": "cc-by",
                            "maintainer": "71421920-fdc8-40fb-ac97-99b85e90b8a7",
                            "methodology": "Other",
                            "methodology_other": "The least cost path method was used here which is "
                            "broken down into two steps: (1) the creation of a "
                            "‘cost’ allocation surface which can also be referred to "
                            "as an effort or friction surface and represents the "
                            "effort to travel across a particular pixel (2) uses the "
                            "cost allocation (or effort surface) in a least cost "
                            "path analysis to estimate travel time from every pixel "
                            "to the nearest destination location (in this case "
                            "health centres). This was done using Dijkstra’s "
                            "algorithm to create Dijkstra trees which find the "
                            "shortest path from one point to another. Full details "
                            "of the method can be found in Watmough et al. (2022) "
                            "and the code available in Zenodo (Hagdorn 2021)\n"
                            "\n"
                            "The cost allocation surface used three primary input "
                            "datasets: (1) land cover (2) roads (3) topography. "
                            "Roads were converted to a 100 m resolution grid with "
                            "the fastest road being given preference when they "
                            "overlap. Any pixels with no road get given the land "
                            "cover. Each pixel is then given a value to represent "
                            "the speed in which an individual can travel across that "
                            "pixel considering the land cover or road type. Each of "
                            "these is then weighted depending on the elevation from "
                            "the DEM, with pixels that have a slope of more than 45 "
                            "degrees being masked from the analysis (ie they are too "
                            "steep for travel). The road types vary for each country "
                            "depending on the Open Street Map and MapwithAI roads so "
                            "the speeds are provided in the data download.\n"
                            "\n"
                            "Travel was assumed to be walking on all road types and "
                            "land cover. For motorised transport see the other "
                            "download for each country.\n"
                            "\n"
                            "[Watmough, et al. "
                            "(2022) Using open-source data to construct 20 metre resolution maps of children’s "
                            "travel time to the nearest health facility, Scientific Data, 9(217)](https://www.nature.com/articles/s41597-022-01274-w)\n"
                            "\n"
                            "[Hagdorn "
                            "(2021)](https://zenodo.org/records/4638563#.YOycpRNKg6g)\n",
                            "name": "algeria-walking-travel-time-to-nearest-level-iv-health-centre",
                            "notes": "A 100 m spatial resolution geotiff of walking travel time in seconds "
                            "to nearest health facility in Algeria. The data was generated using the Child "
                            "Poverty and Access to Services (CPAS) software (10.5281/zenodo.4638563) and was "
                            "created as part of the CPAS project within the Data for Children Collaborative. "
                            "The travel time is calculated assuming walking speeds on all roads, tracks, paths "
                            "and land cover types. A full description is available "
                            "[here](https://doi.org/10.1038/s41597-022-01274-w) a video description of the data "
                            "is also available [here](https://www.dataforchildrencollaborative.com/outputs/presentation-a-100m-resolution-travel-time-map)\n"
                            "\n"
                            "Projection system is: GCS_WGS_1984 EPSG 4326 for all\n",
                            "owner_org": "bb1ac1eb-c322-40f0-b0ea-1696792e61df",
                            "package_creator": "HDX Data Systems Team",
                            "private": False,
                            "subnational": False,
                            "tags": [
                                {
                                    "name": "geodata",
                                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                                },
                                {
                                    "name": "health facilities",
                                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                                },
                                {
                                    "name": "transportation",
                                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                                },
                                {
                                    "name": "africa",
                                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                                },
                            ],
                            "title": "Algeria Walking Travel Time to nearest Level IV health centre",
                        }

                        resources = dataset.get_resources()
                        assert resources == [
                            {
                                "name": "service-area-algeria-walking.tif",
                                "description": "This file is a zip archive containing travel time map in Geotiff "
                                "format and a thumbnail image in PNG format.",
                                "format": "geotiff",
                                "url": "https://s3.eidf.ac.uk/eidf158-walkingtraveltimemaps/service_area_Algeria_walking.tif",
                            }
                        ]
                        for resource in resources:
                            filename = resource["name"]
                            actual = join(tempdir, filename)
                            expected = join(fixtures_dir, filename)
                            assert_files_same(actual, expected)
