
import sys
import os
import pandas
#import pyarrow.parquet as ppq
import fastparquet as ppq
import duckdb
import shutil
import glob
import sqlite3
import logging

logger = logging.getLogger(__name__)

CWD = os.getcwd()
os.environ["TMPDIR"] = f"{CWD}/tmp"


def add_geo(db, tbl, gtype='POINT'):
    db.enable_load_extension(True)
    db.load_extension('mod_spatialite')
    db.enable_load_extension(False)
    cur = db.cursor()

    cur.execute( f"""SELECT st_GeometryType(geomfromwkb(geometry)) FROM {tbl} limit 3""" )
    res = cur.fetchall()
    logger.debug(f"db table {tbl} has geomtype {res}")
    assert res[0][0].upper() == gtype.upper(), f"When adding to {tbl} mixed geometry types found {res[0][0]} when expected {gtype}"
 
    cmds = [
    f"""PRAGMA synchronous=0;""",
    f"""SELECT InitSpatialMetadata('WGS-84');""",
    f"""SELECT AddGeometryColumn("{tbl}", "geom", 4386, '{gtype}', '2');""",
    f"""UPDATE {tbl} SET geom = SetSRID(GeomFromWkb(geometry),4386);""",
    f"""ALTER TABLE {tbl} DROP COLUMN 'geometry';""",
    f"""SELECT CreateSpatialIndex('{tbl}', 'geom');"""
    ]

    for cmd in cmds:
        cur.execute(cmd)
        # wait for the step to complete
        assert cur.fetchall() is not None, f"SQL statement failed {cmd}"

    cur.close()


def copy_duckdb(pqf, dest):
    ddb = duckdb.connect(":memory:")
    rq = ddb.execute("""
    LOAD spatial;""")

    ddb.execute(f"""
    COPY (
    SELECT
           *
      FROM pqf
      ) TO '{dest}'
    WITH (FORMAT GDAL, DRIVER 'GPKG');""")

    return rq


def get_pq_members(pq):
    data = []
    data = glob.glob(pq + "/**")
    return data


def get_db_con(db_prefix, seq=0):
    dbpath = f"{db_prefix}_{seq:03d}.db"
    db = sqlite3.connect(dbpath)

    cur = db.cursor()
    cur.execute("PRAGMA synchronous=0;")
    cur.close()
    return db, dbpath


def read_pq(dbname, tbl, parquetfile, gtype):
    mb = get_pq_members(parquetfile)

    ctr = 1
    for itm in mb:
        db, dbpath = get_db_con(dbname, ctr)
        read_pq_members(db, tbl, itm)
        add_geo(db, tbl, gtype)
        db.commit()
        db.close()

        logger.info(f"imported file {ctr}/{len(mb)}   {itm}")
        ctr += 1

        shutil.move(dbpath, "bkdata")


def read_pq_members(db, tbl, pqm):
    # read the file using fastparquet
    idf = ppq.ParquetFile(pqm)
    colmap = None

    logger.info(f"file: {pqm}")
    rg = 1
    for itm in idf.iter_row_groups():
        # for a read to cleanup any constructor issues
        df = pandas.DataFrame(itm)

        # convert "nan" to N/A
        df.convert_dtypes()

        # flatten the list columns
        for litm in df.columns:
                if litm in ('id', 'updatetime', 'version', 'confidence', 'level', 'geometry'):
                    #skip processing certain columns
                    continue
                df[litm] = df[litm].apply(lambda x: ','.join(str(d) for d in x) if isinstance(x, list) else x)

        cols = df.columns
        for testcol in ('names', 'addresses', 'sources'):
            if not testcol in cols:
                continue
            # search for any row with a a non blank {testcol}
            p = df[testcol].filter(regex='[A-Za-z]$', axis=0)
            if p.shape[0] > 0:
                logger.warning(f"{p.shape[1]} rows with {testcol} in {pqm} row group")

        #df["id"].head()
        #df["categories.main"].head()

        if colmap is None:
            colmap = {}
            for qitm in df.columns:
                if '.' in qitm:
                    xitm = qitm.replace('.','_')
                    colmap[qitm] = xitm

        df = df.rename(columns=colmap)

        # TODO:
        # duckdb spatial is not working yet
        # copy_duckdb(itm, tbl)

        logger.info(f"saving frame of {df.shape} row group {rg}")
        df.to_sql(tbl, db, method='multi', chunksize=1000, if_exists='append')
        rg += 1


def urls():
    # bash  / aws cli to pull the data
    data = """
for thm in admins buildings places transportation; do
  aws s3 cp --region us-west-2 --no-sign-request --recursive s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=${thm} .
done
    """


def theme_config(thm):
    d = {"place": {"file": "rawdata/type=place", "db_prefix": "data/places", "tbl": "places", "gtype": "Point"},
         "segment": {"file": "rawdata/type=segment", "db_prefix": "data/segments", "tbl": "segments", "gtype": "LineString"},
         "connector": {"file": "rawdata/type=connector", "db_prefix": "data/connectors", "tbl": "connectors", "gtype": "Point"},
         "building": {"file": "rawdata/type=building", "db_prefix": "data/buildings", "tbl": "buildings", "gtype": "Polygon"},
         "admin": {"file": "rawdata/type=administrativeBoundary", "db_prefix": "data/admins", "tbl": "admins", "gtype": "LineString"}
    }

    return d[thm]


def parquet_cols(pqm):
    idf = ppq.ParquetFile(pqm)

    logger.info("  columns:")
    for fld in idf.columns:
        logger.info(f"    {fld}")


def parquet_info(pqm):
    idf = ppq.ParquetFile(pqm)
    data = []

    logger.info(f"parquet file {pqm}")
    logger.info(f"   has {idf.info['row_groups']} row groups and {idf.info['rows']:,} rows")
    for idx in range(0,len(idf.row_groups)):
        rg = idf.row_groups[idx]
        logger.info(f"""    row_group: {rg.num_rows:,} rows, compressed ratio  {(rg.total_compressed_size/rg.total_byte_size):0.4g}""")
        data.append({"rows": idf.row_groups[idx].num_rows,
                    "compression": (rg.total_compressed_size/rg.total_byte_size)
                    })

    return {"file": pqm, 
            "rows": idf.info["rows"],
            "row_groups": idf.info['row_groups'],
            "rg_info": data
            }


def review_parquet_info(thm_file):

    iqx = 0
    for itm in get_pq_members(thm_file):
        if iqx == 0:
            parquet_cols(itm)
            iqx = 1
        parquet_info(itm)



def get_themes(thm):
    d = theme_config(thm)
    return d


def get_theme_status(thm):
    config = get_themes()[thm]

    dbx = f"""{config["db_prefix"]}_001.db"""

    if not os.path.exists(dbx):
        return False
    
    return True


def main(args):
    theme = "place"
    #theme = "admin"
    theme = "connector"
    #theme = "segment"
    config = theme_config(theme)

    #review_parquet_info(config["file"])
    read_pq(config["db_prefix"], config["tbl"], config["file"], config["gtype"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main(sys.argv)