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

## Notes
* pyarrow.lib.ArrowNotImplementedError: Not implemented type for Arrow list to pandas: map<string, string ('array_element')>

* pyarrow.lib.ArrowNotImplementedError: Function 'dictionary_encode' has no kernel matching input types (map<string, list<array_element: map<string, string ('array_element')>> ('names'         
        


* "[('common', array([list([('value', 'Make a memory photobooth'), ('language', 'local')])],\n      dtype=object))]"


names = [('common', array([list([('value', 'Fibras Capilares en Argentina'), ('language', 'local')])], dtype=object))]

categories = {'main': 'beauty_salon', 'alternate': array(['health_and_medical', 'hospital'], dtype=object)}

brand = {'names': None, 'wikidata': None}
brand = {'names': [('brand_names_common', array([list([('value', 'コメリ'), ('language', 'local')])], dtype=object))], 'wikidata': None}

addresses = [list([('locality', 'Ciudad de Buenos Aires'), ('postcode', '1431'), ('freeform', 'José Pascual Tamborini 5630'), ('region', 'C'), ('country', 'AR')])]

sources = [list([('dataset', 'meta'), ('property', ''), ('recordid', '416537698693731')])]

bbox = {'minx': -58.4976, 'maxx': -58.4976, 'miny': -34.56933, 'maxy': -34.56933}
