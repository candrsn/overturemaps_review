#OvertureMaps Alpha Release 2023-07-26 analysis

##Themes (and geometry type)
* Admins (Linestring)
* Places (Point)
* Roads
 * Segments (Linestring)
 * Connectors (Linestring)

##Flattened Data Structure (as SQLITE3 / SPATIALITE)

* Admins
 * CREATE TABLE IF NOT EXISTS "admins" (
"index" INTEGER,
  "id" TEXT,
  "updatetime" TEXT,
  "version" INTEGER,
  "names" TEXT,
  "adminlevel" INTEGER,
  "maritime" TEXT,
  "subtype" TEXT,
  "localitytype" TEXT,
  "context" TEXT,
  "isocountrycodealpha2" TEXT,
  "isosubcountrycode" TEXT,
  "defaultlanugage" TEXT,
  "drivingside" TEXT,
  "sources" TEXT,
  "bbox_minx" REAL,
  "bbox_maxx" REAL,
  "bbox_miny" REAL,
  "bbox_maxy" REAL
, "geom" LINESTRING);

* Places
 * CREATE TABLE IF NOT EXISTS "places" (
"index" INTEGER,
  "id" TEXT,
  "updatetime" TEXT,
  "version" INTEGER,
  "names" TEXT,
  "confidence" REAL,
  "websites" TEXT,
  "socials" TEXT,
  "emails" TEXT,
  "phones" TEXT,
  "addresses" TEXT,
  "sources" TEXT,
  "categories_main" TEXT,
  "categories_alternate" TEXT,
  "brand_names" TEXT,
  "brand_wikidata" TEXT,
  "bbox_minx" REAL,
  "bbox_maxx" REAL,
  "bbox_miny" REAL,
  "bbox_maxy" REAL
, "geom" POINT);


* Segments
 * CREATE TABLE IF NOT EXISTS "segments" (
"index" INTEGER,
  "id" TEXT,
  "updatetime" TIMESTAMP,
  "version" INTEGER,
  "level" INTEGER,
  "subtype" TEXT,
  "connectors" TEXT,
  "road" TEXT,
  "sources" TEXT,
  "bbox_minx" REAL,
  "bbox_maxx" REAL,
  "bbox_miny" REAL,
  "bbox_maxy" REAL
, "geom" LINESTRING);

* Connectors
 * CREATE TABLE IF NOT EXISTS "connectors" (
"index" INTEGER,
  "id" TEXT,
  "updatetime" TIMESTAMP,
  "version" INTEGER,
  "level" INTEGER,
  "subtype" TEXT,
  "connectors" TEXT,
  "road" TEXT,
  "sources" TEXT,
  "geometry" TEXT,
  "bbox_minx" REAL,
  "bbox_maxx" REAL,
  "bbox_miny" REAL,
  "bbox_maxy" REAL
, "geom" LINESTRING);



# Data Stride  (Parquet Row Groups)
* Admin
* Places
* Segments -
* Connectors  - 2090100