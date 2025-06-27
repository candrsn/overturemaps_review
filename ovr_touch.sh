

ogr2ogr -of gpkg test.gpkg "/vsis3//overturemaps-us-west-2/release/2025-06-25.0/theme=addresses/type=address/" -sql "SELECT * FROM type=address WHERE country = 'US' " 


ogr2ogr -of csv test.csv "/vsis3//overturemaps-us-west-2/release/2025-06-25.0/theme=addresses/type=address/" -sql "SELECT * FROM \"type=address\" WHERE country = 'US' limit 50 " 

