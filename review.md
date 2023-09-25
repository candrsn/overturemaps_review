

# Geospatial

```
sqlite> select count(*), geometrytype(geomfromwkb(geometry)) from buildings group by 2;
                           count(*) = 10112
geometrytype(geomfromwkb(geometry)) = MULTIPOLYGON

                           count(*) = 8002474
geometrytype(geomfromwkb(geometry)) = POLYGON
sqlite> select count(*), geometrytype(st_multi(geomfromwkb(geometry))) from buildings group by 2;
                                     count(*) = 8012586
```
