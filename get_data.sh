#!/bin/bash
#
#

scan_coretypes() {
aws s3 ls  --region us-west-2 --no-sign-request  s3://overturemaps-us-west-2/release/${REL}/ | sed -e 's/PRE theme=//' | sed -e 's/\///'
}


coretypes() {
  # exclude the base theme
  echo "$(scan_coretypes)" | grep -v base | grep -v buildings

}


extratypes() {
  echo '
    
  '
}


fixup() {
  for idir in 'admins' 'base' 'buildings' 'places' 'transportation'; do
    mkdir 'theme='$idir | :
  done

  #Admins
  for idx in 'administrativeBoundary' 'locality' 'localityArea'; do
    mv 'type='$idx 'theme=admins'
  done

  #base
  for idx in land landUse water ; do
    mv 'type='$idx 'theme=base'
  done

  #buildings
  for idx in building part; do
    mv 'type='$idx 'theme=buildings'
  done

  #transportation
  for idx in segment connector; do
    mv 'type='$idx 'theme=transportation'
  done

  #places
  for idx in place; do
    mv 'type='$idx 'theme=places'
  done

}

duckdb_test() {

    # s3://overturemaps-us-west-2/release/
    # SELECT count(*) FROM read_parquet('s3://overturemaps-us-west-2/release/2025-03-19.1/theme=addresses/type=address/part-00000-38196609-a377-4942-b72d-8f165a26d089-c000.zstd.parquet');

    # just make this a no-op for now
    :
}


REL=''
REL='2025-06-25.0'
#REL='2025-04-23.0'
RD=''

if [ -z "$RD" ]; then
  RD=$REL
fi

# protect against re-runs when the rawdata dir already exists
mkdir -p rawdata/$RD
pushd rawdata/$RD

for thm in $(coretypes); do
  echo "starting download of theme=${thm}/" >&2
  aws s3 sync --region us-west-2 --no-sign-request  s3://overturemaps-us-west-2/release/$REL/theme=${thm}/ theme=${thm}/
done

for thm in $(extratypes); do
  if [ -n "$thm" ]; then
  echo "starting download of theme=${thm}/" >&2
  aws s3 sync --region us-west-2 --no-sign-request  s3://overturemaps-us-west-2/release/$REL/theme=${thm}/ theme=${thm}/
  fi
done

popd
