# theme=transportation/type=segment/part-00040-a82a47c4-95f0-40aa-a9b1-e787b12eff8b-c000.zstd.parquet
# s3://overturemaps-us-west-2/release/2024-04-16-beta.0/theme=transportation/type=segment/
# https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-04-16-beta.0/theme=transportation/type=segment/part-00040-a82a47c4-95f0-40aa-a9b1-e787b12eff8b-c000.zstd.parquet

DPFX="rawdata/20240416/"
SPAT="114.30252 30.46633 114.48085 30.60909"
TL1=theme=transportation/type=segment/part-00041-a82a47c4-95f0-40aa-a9b1-e787b12eff8b-c000.zstd.parquet
TL2=theme=transportation/type=segment/part-00040-a82a47c4-95f0-40aa-a9b1-e787b12eff8b-c000.zstd.parquet
TU=overturemaps-us-west-2/release/2024-04-16-beta.0/

time ogrinfo -al -so -spat $SPAT ${DPFX}$TL1
#Spatial select $TL1 5 sec
# should be 14612 features

time ogrinfo -al -so -spat $SPAT ${DPFX}$TL2
#Spatial select $TL2 7 sec
# should be 0 features

time ogr2ogr ts_all.gpkg  $TL1
#convert all of TL1 to GPKG  73 sec

time ogr2ogr -spat 114.30252 30.46633 114.48085 30.60909 ts_spat.gpkg  $TL1
#convert spat of TL1 to GPKG  7 sec

# read the file metadata
time AWS_NO_SIGN_REQUEST=YES ogrinfo -al -so  /vsis3/${TU}${TL1} --debug on
# 1.8 sec includeing IOWAIT on Downloads
# 0.3 sec in user space

# try a spatial filter
time AWS_NO_SIGN_REQUEST=YES ogrinfo -al -so  /vsis3/${TU}${TL1} -spat $SPAT --debug on
# 78 sec includeing IOWAIT on Downloads
# 10 sec in user space


qgis /vsis3/overturemaps-us-west-2/release/2024-04-16-beta.0/theme=transportation/type=segment/part-00041-a82a47c4-95f0-40aa-a9b1-e787b12eff8b-c000.zstd.parquet
# works somewhat

