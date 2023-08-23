
import sys
import sqlite3
import pandas
import geopandas
import glob
import logging

logger = logging.getLogger(__name__)


def enable_geo(db):
    db.enable_load_extension(True)
    db.load_extension("mod_spatialite")
    #more flexible way
    #db.execute("select load_extension('{0}');".format(shared_lib))
    db.enable_load_extension(False)


def read_data(db):

    # SQL must wrap the geometry in hex(st_asbinary(...))
    sql = "SELECT *, Hex(ST_AsBinary(GEOMETRY)) as xgeom FROM cities;"
    df = geopandas.GeoDataFrame.from_postgis(sql, db, geom_col="xgeom")

    print(df.head())
    print(df.columns)
    return db


def get_data_files(theme):
    data = glob.glob(f"bxdata/{theme}*.db")
    return data


def map_data(plt, df):
    # set a variable that will call whatever column we want to visualise on the map
    var = 'POP' # set the range for the choropleth

    if plt is not None:
        plt = df.plot(var, kind="geo")
    else:
        plt = df.plot(var, kind="geo", ax=plt)

    plt.savefig("map_export.png", dpi=300)


def data_themes():
    data = [
        ["admin", "admin"],
        ["places", "places"],
        ["segments", "segments"],
        ["segments", "connectors"]
    ]
    return data


def main(args):
    # themes = ["admins", "places", "segments"] 
    dbs = get_data_files("segments")
    for itm in dbs:
        db = sqlite3.connect(itm)
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv)
