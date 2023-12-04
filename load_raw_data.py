import os
import urllib.request as request
from pathlib import Path

import geopandas as gpd
from open_mastr import Mastr
from sqlalchemy import create_engine


def main():
    """Download the Marktstammdatenregister and save it to the database that was created by docker-compose."""

    engine = get_engine()
    download_mastr(engine)
    download_districts_geoboundaries(engine=engine)
    download_municipalities_geoboundaries(engine=engine)


def download_mastr(engine):
    db = Mastr(engine=engine)
    db.download(date="existing")


def get_engine():
    POSTGRES_DB = "verify-marktstammdatenregister"
    POSTGRES_USER = "postgres"
    POSTGRES_PASSWORD = "postgres"
    PORT = "5512"
    return create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{PORT}/{POSTGRES_DB}"
    )


def download_districts_geoboundaries(engine) -> gpd.GeoDataFrame:
    constants = {
        "url": "https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_1231/aktuell/vg5000_12-31.utm32s.shape.ebenen.zip",
        "table_name": "districts_geoboundaries",
        "download_path": Path.home() / ".verify-marktstammdaten",
        "zipfile_path": "vg5000_12-31.utm32s.shape.ebenen/vg5000_ebenen_1231/VG5000_KRS.shp",
        "filename": "vg5000_1231.zip",
    }
    load_geoboundaries(constants, engine)


def download_municipalities_geoboundaries(engine) -> gpd.GeoDataFrame:
    constants = {
        "url": "https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_1231/aktuell/vg5000_12-31.utm32s.shape.ebenen.zip",
        "table_name": "municipalities_geoboundaries",
        "download_path": Path.home() / ".verify-marktstammdaten",
        "zipfile_path": "vg5000_12-31.utm32s.shape.ebenen/vg5000_ebenen_1231/VG5000_GEM.shp",
        "filename": "vg5000_1231.zip",
    }
    load_geoboundaries(constants, engine)


def download_from_url(
    url: str, save_directory: str, filename: str, overwrite: bool = False
) -> None:
    """Downloads a file from a given url and saves it to the given path

    Parameters
    ----------
    url : str
        url to download from.
    save_directory : str
        folder path where the file is saved.
    filename : str
        name of the file.
    """

    save_path = os.path.join(save_directory, filename)

    if not overwrite and os.path.isfile(save_path):
        print(f"File {filename} already downloaded.")
        return None
    create_directories([save_directory])
    print(url, save_path)
    request.urlretrieve(url, save_path)


def create_directories(directory_list):
    for directory in directory_list:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)


def load_geoboundaries(constants, engine) -> gpd.GeoDataFrame:
    download_path = constants["download_path"]
    filename = constants["filename"]
    zipfile_path = constants["zipfile_path"]
    download_from_url(
        url=constants["url"],
        save_directory=download_path,
        filename=filename,
    )
    zipfile = f"{download_path}/{filename}!{zipfile_path}".replace("\\", "/").replace(
        "C:/", "zip:///"
    )
    gdf = gpd.read_file(zipfile)
    gdf.to_postgis(name=constants["table_name"], con=engine)


if __name__ == "__main__":
    main()
