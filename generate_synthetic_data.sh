#!/usr/bin/env bash
./run_synthea -c synthea.properties -m congestive_heart_failure:breast_cancer -p 500 -s 0 -cs 0 -r 20210401

find ./output -name "*.ndjson" -size +50000000c -print | while read file; do
  fn=$(basename -s .ndjson ${file})
  dn=$(dirname ${file})
  split --additional-suffix=.ndjson --elide-empty-files --line-bytes=50000000 --numeric-suffixes --suffix-length=2 $file ${dn}/${fn}. && rm $file
done
