

-- DUCKDB
.	
CREATE VIEW ttxs as SELECT * FROM read_parquet('/usbmedia/gis_data/data/overturemaps/rawdata/2025-05-21.0/theme=addresses/type=address/part-*.parquet');

LOAD SPATIAL;

CREATE TABLE us_addresses as SELECT id, number, unit, street, postcode,
   address_levels->>'$[0].value' as state, address_levels->>'$[1].value' as city, postal_city, geometry
   FROM read_parquet('/usbmedia/gis_data/data/overturemaps/rawdata/2025-05-21.0/theme=addresses/type=address/part-*.parquet')
   WHERE country = 'US' and GEOMETRY is not NULL;

SELECT st_astext(geometry) from us_addresses LIMIT 50;
SELECT count(*), st_isvalid(pgeometry) FROM (SELECT GEOMETRY FROM main.us_addresses limit 5) as p GROUP BY 2;


SELECT address_levels->>'$[0].value' as state, address_levels->>'$[1].value' as city, address_levels FROM us_addresses limit 5;


SELECT json_array_length(address_levels), count(*) FROM us_Addresses GROUP BY 1;


LOAD SQLITE;
ATTACH 'us_add.db' (TYPE sqlite);

CREATE TABLE us_add.us_addresses AS SELECT id, number, unit, street, postcode, state, city, postal_city, 
    CAST(geometry as geometry) as g2
  FROM main.us_addresses;


LOAD spatial; -- noqa

SET s3_region='us-west-2';

COPY ( SELECT id, number, unit, street, postcode,
   address_levels->>'$[0].value' as state, address_levels->>'$[1].value' as city, postal_city, geometry
   FROM read_parquet('/usbmedia/gis_data/data/overturemaps/rawdata/2025-05-21.0/theme=addresses/type=address/part-*.parquet') 
   WHERE country = 'US'
) TO 'overture_data.db' WITH (FORMAT GDAL, DRIVER 'SQLITE');


COPY (SELECT id, number, unit, street, postcode, state, city, postal_city, ST_geomfromwkb(geometry) from us_addresses
) TO 'overture_data.db' WITH (FORMAT GDAL, DRIVER 'SQLITE');

COPY (SELECT id, number, unit, street, postcode, state, city, postal_city, geometry from us_addresses
) TO 'overture_data.db' WITH (FORMAT GDAL, DRIVER 'SQLITE');



ogr2ogr -of sqlite -sql "SELECT *, st_geomfromwkb(wkb) as geometry from us_addresses" us_addx.db us_add.db -nln us_addresses

ogrinfo us_addx.db -sql "CREATE INDEX ON us_addresses USING geometry"


echo "
.load mod_spatialite
SELECT INITSpatialMetadata('wgs84');

ALTER TABLE us_addresses rename column geometry to g2;
SELECT AddGeometryColumn('us_addresses', 'geometry', 4326, 'Point');

UPDATE us_addresses SET geometry = CAST(g2 as GEOMETRY);
SELECT CreateSpatialIndex('us_addresses', 'geometry');

" | sqlite3 us_add.db


rm cache/us_address_direct_qry.db | :
time ogr2ogr -of sqlite -sql 'SELECT id, number, unit, street,
   json_extract(address_levels, "$[1].value") as city, 
   json_extract(address_levels, "$[0].value") as state, 
   postal_city, postcode, country, 
   geometry
   FROM "type=address"
   WHERE country = "US" and 
     GEOMETRY is NOT NULL and
     street is NOT NULL '   -nln us_addresses -preserve_fid -gt 5000000 -dialect SQLITE cache/us_address_direct_qry.db /usbmedia/gis_data/data/overturemaps/rawdata/2025-05-21.0/theme=addresses/type=address -overwrite -dsco SPATIALITE=NO

time ogr2ogr -of sqlite  -where " country = 'US' " cache/us_address_spatialite.db  /usbmedia/gis_data/data/overturemaps/rawdata/2025-05-21.0/theme=addresses/type=address/ -select id,number,unit,street,postal_city,postcode,address_levels -nln us_addresses -preserve_fid -gt 5000000 -overwrite -dsco SPATIALITE=YES

time ogr2ogr -of parquet  -where " country = 'US' " cache/us_address.pq  /usbmedia/gis_data/data/overturemaps/rawdata/2025-05-21.0/theme=addresses/type=address/ -select id,number,unit,street,postal_city,postcode,address_levels -nln us_addresses -preserve_fid -gt 5000000 -overwrite -lco COMPRESSION=ZSTD -lco ROW_GROUP_SIZE=68000

time ogr2ogr -of sqlite  -where " country = 'US' " cache/us_address_sqlite.db  /usbmedia/gis_data/data/overturemaps/rawdata/2025-05-21.0/theme=addresses/type=address/ -select id,number,unit,street,postal_city,postcode,address_levels -nln us_addresses -preserve_fid -gt 5000000 -overwrite 

rm  cache/us_address_test.pq
time ogr2ogr -of parquet  -where " country = 'US' " cache/us_address_test.pq  cache/us_address_direct_qry.db -nln us_addresses -preserve_fid -gt 5000000 -overwrite -lco COMPRESSION=ZSTD -lco ROW_GROUP_SIZE=68000

time (
rm cache/us_address_duck.ddb | :
echo "
ATTACH DATABASE 'cache/us_address_direct_qry_sqlite.db' as sdb;
.timer on

CREATE TABLE main.us_addresses as SELECT * FROM sdb.us_addresses;

CHECKPOINT;

" | duckdb cache/us_address_duck.ddb  )


time (
echo "
  


" | duckdb cache/us_address_duck.ddb  )

