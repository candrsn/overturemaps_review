#!/bin/bash
#
#


for thm in admins buildings places transportation; do
  aws s3 cp --region us-west-2 --no-sign-request --recursive s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=${thm} .

done

