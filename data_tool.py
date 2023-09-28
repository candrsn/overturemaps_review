
import sys
import os
import pandas
import pyarrow.parquet as paq
import pyarrow as pa

import parquet
import fastparquet as ppq

import duckdb
import shutil
import glob
import sqlite3
import json
import re
import numpy
import logging

logger = logging.getLogger(__name__)

CWD = os.getcwd()
os.environ["TMPDIR"] = f"{CWD}/tmp"


def add_geo(db, tbl, gtype='POINT'):
    logger.info(f"Adding OSGeo support data and converting the geometry column for {tbl}")
    db.enable_load_extension(True)
    db.load_extension('mod_spatialite')
    db.enable_load_extension(False)
    cur = db.cursor()

    cur.execute( f"""SELECT st_GeometryType(GeomFromWkb(geometry)), count(*) FROM {tbl} GROUP BY 1""" )
    res = cur.fetchall()
    g = [d[0].upper() for d in res]
    logger.info(f"db table {tbl} has geomtypes \n {res}")
    assert res[0][0].upper() in g, f"When adding to {tbl} mixed geometry types found {res[0][0]} when expected {gtype}"

    cur.close()
    cmds = [
    f"""PRAGMA synchronous=0;""",
    f"""SELECT InitSpatialMetadata('WGS-84');""",
    f"""SELECT AddGeometryColumn("{tbl}", "geom", 4386, '{gtype}', '2');"""
    ]

    if gtype.upper().startswith('MULTI'):
        # force into multi <geom>
        cmds.append(f"""UPDATE {tbl} SET geom = SetSRID(ST_Multi(GeomFromWkb(geometry)),4386);""")
    else:
        cmds.append(f"""UPDATE {tbl} SET geom = SetSRID(GeomFromWkb(geometry),4386);""")

    cmds += [
    f"""ALTER TABLE {tbl} DROP COLUMN 'geometry';""",
    #f"""VACUUM main;""",
    f"""SELECT CreateSpatialIndex('{tbl}', 'geom');"""
    ]


    for cmd in cmds:
        cur = db.cursor()
        cur.execute(cmd)
        # wait for the step to complete
        assert cur.fetchall() is not None, f"SQL statement failed {cmd}"

        # close the cursor and commit work to this point
        cur.close()
        db.commit()



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


def check_output_status(dbpath):
    if os.path.exists(dbpath.replace("data/","bkdata/")):
        return True

    return False


def read_pq(dbname, tbl, parquetfile, gtype, engine='pyarrow', force=False, config=None):
    mb = get_pq_members(parquetfile)
    logger.info(f"reading {len(mb)} parquet files into {tbl}")

    ctr = 0
    for itm in mb:
        ctr += 1
        db, dbpath = get_db_con(dbname, ctr)
        if check_output_status(dbpath) is True and force is False:
            # if an output file already exists skip processing this file
            continue

        if engine == 'pyarrow':
            read_pq_members_pyarrow(db, tbl, itm)
        elif engine == 'fastparquet':
            read_pq_members_fastparquet(db, tbl, itm)
        else:
            read_pq_members_parquet(db, tbl, itm)

        if gtype is not None:
            add_geo(db, tbl, gtype)

        db.commit()
        db.close()

        logger.info(f"imported file {ctr}/{len(mb)}   {itm}")

        try:
            shutil.move(dbpath, "bkdata")
        except shutil.Error as e:
            logger.warning(f"failed to move {dbpath} to its final location")
            logger.warning(f"{e}")


def build_colmap(df, colmap=None):
    if colmap is None:
        colmap = {}
        for qitm in df.columns:
            if '.' in qitm:
                xitm = qitm.replace('.','_')
                colmap[qitm] = xitm

    return colmap

def nd_key_dict(data, tbl):
    return data


def nd_multkey(data, tbl):
    return data

def kvlist_dict(data):
    odata = {}
    for itm in data:
       odata[itm[0]] = itm[1]

    return odata


def pq_to_json(data, tbl, dcol, typeinfo):
    ovals = None

    # convert numpy.ndobjects to JSON
    # some of these are stuctured maps as a handler for dicts
    stype = str(typeinfo.type)

    if not (type(data) is list or isinstance(data, list) or type(data) is dict or isinstance(data, numpy.ndarray)):
        if data is None:
            return data
        # skip simple types
        return data
    elif stype == "struct<names: map<string, list<array_element: map<string, string ('array_element')>> ('names')>, wikidata: string>":
        # skip cases where all of the keys have None values
        kys = data.keys()
        # if none of the top level keys and not None....
        if not True in [data[dk] is None for dk in kys]:
            ovals = None
        else:
            qdata = {}
            for topkey in kys:
                # grab data from all keys that have data
                if not (data[topkey] is None):
                    if type (data[topkey]) in (str, int, float):
                        qdata[topkey] = data[topkey]
                    else:
                        qdata[topkey] = kvlist_dict(data[topkey][0][1][0])
            if len(qdata.keys()) == 0:
                ovals = None
            else:
                ovals = json.dumps(qdata)
    elif stype == 'struct<main: string, alternate: list<array_element: string>>':
        # skip cases where all of the keys have None values
        kys = data.keys()
        # if none of the top level keys are not None....
        if not True in [data[dk] is None for dk in kys]:
            ovals = None
        else:
            qdata = {}
            for topkey in kys:
                # grab data from all keys that have data
                if not (data[topkey] is None):
                    if type (data[topkey]) in (str, int, float):
                        qdata[topkey] = data[topkey]
                    else:
                        qdata[topkey] == ""
            ovals = json.dumps(qdata)
    elif stype == "map<string, list<array_element: map<string, string ('array_element')>> ('names')>":
        qdata = {}
        for q in data:
            qxdata = {}
            for qx in q[1][0]:
                qxdata[qx[0]] = qx[1]
            qdata[q[0]] = qxdata
        if len(qdata.keys()) == 0:
            ovals = None
        else:
            ovals = json.dumps(qdata)
    elif stype == "list<array_element: map<string, string ('array_element')>>":
        qdata = {}
        for q in data[0]:
            qdata[q[0]] = q[1]
        
        if len(qdata.keys()) == 0:
            ovals = None
        else:
            ovals = json.dumps(qdata)
    elif dcol in ("socials", "phones"):
        # simplified case to get to None
        if data.get('names') is None:
            return data
        ovals = data
    elif dcol == "addresses":
        if data["alternate"] is None:
            ovals = json.dumps(data)
        else:
            ovals= json.dumps(data)
    elif dcol == "categories":
        if data["alternate"] is None:
            ovals = json.dumps(data)
        else:
            data['alternate'] = list(data['alternate'])
            ovals = json.dumps(data)
    elif stype == "map<string, list<array_element: map<string, string ('array_element')>> ('names')>":
        if len(data) == 0:
            # convert empty lists to Null
            return None

        # walk through all for the key value pairs
        qdata = {}
        for xitm in data:
            ky = xitm[0]
            #TODO:
            # build dicts from the arrays
            ditm = {}
            for nitm in xitm[1][0]:
                ditm[nitm[0]] = nitm[1]
            qdata[ky]= ditm

            ovals = json.dumps(qdata)
    elif stype == 'struct<main: string, alternate: list<array_element: string>>':
        key = data[0][0]
        vals = {}
        for idx in data[0][1][0]:
            ky = idx[0]
            val = idx[1]
            vals[ky] = val

        if not key == "common":
            logger.info(f"odd key {key}")
        ovals = {key: vals}
        ovals = json.dumps(ovals)
    elif type(data) is dict:
        # attempt a simple conversion if nothig else caught the conversion
        kys = data.keys()
        # if none of the top level keys are not None....
        if not True in [data[dk] is None for dk in kys]:
            ovals = None
        else:
            ovals = json.dumps(data)
    else:
        ovals = str(data)

    return ovals


def report_col_tests(df, tests, tbl="table"):
    cols = df.columns
    for testcol in tests:
        if not testcol in cols:
            continue
        # search for any row with a a non blank {testcol}
        p = df.isnull().any()
        # df.isnull().sum()
        if p[testcol] is False:
            logger.warning(f"{tbl} :: {testcol} has rows with Nulls")


def save_pq_frame(db, tbl, df, colmap=None, schema=None, convtype="JSON", engine='pyarrow'):
    # convert "nan" to N/A
    df.convert_dtypes()

    if engine == 'pyarrow':
        # flatten the list columns
        for litm in df.columns:

                if (not isinstance(df.dtypes[litm], object)) or litm in ('id', 'updatetime', 'version', 'confidence', 'level', 'height', 'numfloors', 'geometry'):
                    # skip processing certain named columns
                    # and simple columns
                    continue

                if schema is None:
                    col_num = list(df.columns).index(litm)
                    stype = df.dtypes[litm]
                else:
                    # flatten lists and dicts into JSON (str)
                    col_num = schema.names.index(litm)
                    stype = schema[col_num]
                    #stype.flatten()

                logger.debug(f"Column {litm} in {tbl} is {stype.type}")

                if convtype == "JSON":
                    #df[litm] = df[litm].apply(lambda x: pq_to_json(x, tbl, litm, stype) if isinstance(x, list) or isinstance(x, dict) else str(x))
                    df[litm] = df[litm].apply(lambda x: pq_to_json(x, tbl, litm, stype) if isinstance(x, list) or isinstance(x, dict) or isinstance(x, object) else str(x))
                elif convtype == "string":
                    # convert the KEY Mapped data to string representation using the shallow method
                    df[litm] = df[litm].apply(lambda x: str(x) if isinstance(x, list) or isinstance(x, dict) else str(x))
                elif convtype == "text":
                    # use the deep conversion method to string
                    df[litm] = df[litm].apply(lambda x: f"{x}" if isinstance(x, list) or isinstance(x, dict) else str(x))

    report_col_tests(df, ('names', 'addresses', 'sources'), tbl=tbl)

    if colmap is None:
        colmap = build_colmap(df, colmap)

    df = df.rename(columns=colmap)

    logger.info(f"saving frame of {df.shape} to {tbl}")
    df.to_sql(tbl, db, method='multi', chunksize=1000, if_exists='append')


def read_pq_members_parquet(db, tbl, pqm):
    # read the file using pyarrow
    idf = parquet.ParquetFile(pqm)
    colmap = None

    logger.info(f"file: {pqm}")
    rg = 1
    for idx in range(0,idf.metadata.num_row_groups):
        itm = idf.read_row_group(idx)
        # for a read to cleanup any constructor issues
        ddf = itm["names"].to_string()
        #itm["names"] = ddf
        #df = itm.to_pandas()
        df = itm.to_pylist()
        df = pandas.DataFrame(df)

        if colmap is None:
            colmap = build_colmap(df, colmap)

        save_pq_frame(db, tbl, df, colmap)
        rg += 1


def read_pq_members_pyarrow(db, tbl, pqm):
    # read the file using pyarrow
    idf = pa.parquet.ParquetFile(pqm, memory_map=True)
    # idf = pa.parquet.ParquetFile(pqm, memort_map=True, read_dictionary=[<dictcols>])
    colmap = None

    logger.info(f"parquet file: {pqm}")
    rg = 1
    for idx in range(0,idf.metadata.num_row_groups):
        itm = idf.read_row_group(idx)
        # Use a reader to cleanup any constructor issues

        schema = itm.schema

        # need to bounce through a RecordBatchReader to make the conversion work
        df = itm.to_reader().read_pandas()

        if colmap is None:
            colmap = build_colmap(df, colmap)

        colmap = save_pq_frame(db, tbl, df, colmap=colmap, schema=schema)
        rg += 1


def read_pq_members_fastparquet(db, tbl, pqm):
    # read the file using fastparquet
    idf = ppq.ParquetFile(pqm)
    coltypes = {k: str for k in idf.columns}
    # reopen the file declaring the expected dtypes
    idf = ppq.ParquetFile(pqm, dtypes=coltypes)

    colmap = None

    logger.info(f"parquet file: {pqm}")
    rg = 1
    for itm in idf.iter_row_groups():
        # Use a read to cleanup any constructor issues
        df = pandas.DataFrame(itm)

        if colmap is None:
            colmap = build_colmap(df, colmap)

        colmap = save_pq_frame(db, tbl, df, colmap)
        rg += 1


def urls():
    # bash  / aws cli to pull the data
    data = """
for thm in admins buildings places transportation; do
  aws s3 cp --region us-west-2 --no-sign-request --recursive s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=${thm} .
done
    """


def theme_config(thm):
    d = {"place": {"file": "rawdata/type=place", "db_prefix": "data/places", "tbl": "places", "gtype": "Point", "engine": "pyarrow"},
         "segment": {"file": "rawdata/type=segment", "db_prefix": "data/segments", "tbl": "segments", "gtype": "LineString", "engine": "fastparquet"},
         "connector": {"file": "rawdata/type=connector", "db_prefix": "data/connectors", "tbl": "connectors", "gtype": "Point", "engine": "pyarrow"},
         "building": {"file": "rawdata/type=building", "db_prefix": "data/buildings", "tbl": "buildings", "gtype": "MultiPolygon", "engine": "pyarrow"},
         "admin": {"file": "rawdata/type=administrativeBoundary", "db_prefix": "data/admins", "tbl": "admins", "gtype": "LineString", "engine": "pyarrow"}
    }

    return d[thm]


def parquet_cols(pqm):
    idf = ppq.ParquetFile(pqm)

    logger.info("  columns:")
    for fld in idf.columns:
        logger.info(f"    {fld}")


def parquet_info_pyarrow(pqm):
    # PyArrow.Parquet does not like this without memory_map on
    idf = pa.parquet.ParquetFile(source=pqm, memory_map=True, read_dictionary=["names","addresses"])

    data = []

    logger.info(f"parquet file {pqm}")
    logger.info(f"   has {idf.metadata.num_row_groups} row groups and {idf.metadata.num_rows:,} rows")
    for idx in range(0,idf.num_row_groups):
        rg = idf.read_row_group(idx)
        logger.info(f"""    row_group: {rg.num_rows:,} rows""")
        data.append({"rows": rg.num_rows,
                    "compression": 1.0
                    })

    return {"file": pqm,
            "rows": idf.metadata.num_rows,
            "row_groups": idf.metadata.num_row_groups,
            "rg_info": data
            }


def parquet_info(pqm):
    # fastparquet
    idf = ppq.ParquetFile(pqm)

    data = []

    logger.info(f"parquet file {pqm}")
    logger.info(f"   has {idf.info['row_groups']} row groups and {idf.info['rows']:,} rows")
    for idx in range(0,len(idf.row_groups)):
        rg = idf.row_groups[idx]
        logger.info(f"""    row_group: {rg.num_rows:,} rows, compressed ratio  {(rg.total_compressed_size/rg.total_byte_size):0.4g}""")
        data.append({"rows": rg.num_rows,
                    "compression": (rg.total_compressed_size/rg.total_byte_size)
                    })

    return {"file": pqm,
            "rows": idf.info["rows"],
            "row_groups": idf.info['row_groups'],
            "rg_info": data
            }


def review_parquet_info(thm_file, use_pyarrow=False):

    iqx = 0
    for itm in get_pq_members(thm_file):
        if iqx == 0:
            parquet_cols(itm)
            iqx = 1
        if use_pyarrow is True:
            parquet_info_pyarrow(itm)
        else:
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
    pq_engine = 'pyarrow'
    #usePyarrow = 'parquet'
    #usePyarrow = 'fastparquet'
    #theme = "place"
    #theme = "admin"
    #theme = "connector"
    #theme = "segment"
    theme = "building"
    config = theme_config(theme)

    #review_parquet_info(config["file"], use_pyarrow=False)
    read_pq(config["db_prefix"], config["tbl"], config["file"], config["gtype"], engine=config['engine'], force=True, config=config)

    logger.info("All Done")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main(sys.argv)