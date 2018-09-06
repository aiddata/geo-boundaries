#!/bin/bash



# rename extensions to proper format

cd /sciclone/aiddata10/REU/geoboundaries/data
cd 1_3_0/zip

for i in geojson/*; do
    for j in $i/*; do
        mv $j ${j/zip/geojson.zip}
    done
done

for i in geojson_simple/*; do
    for j in $i/*; do
        mv $j ${j/zip/geojson.zip}
    done
done

for i in shapefile/*; do
    for j in $i/*; do
        mv $j ${j/zip/shp.zip}
    done
done


# unzip files
# only move old metadata for first one of them, just delete other duplicated

cd /sciclone/aiddata10/REU/geoboundaries/data
cd 1_3_0/zip

mkdir -p ../final/metadata

for i in geojson/*; do
    mkdir -p ../final/$i
    for j in $i/*; do
        unzip -o $j -d ../final/$i
        metadata=$(ls ../final/$i/*metadata*)
        k=$(basename $j)
        iso_adm="${k:0:8}"
        ext="${metadata##*.}"
        mv $metadata ../final/metadata/"$iso_adm"."$ext"
    done
done


for i in geojson_simple/*; do
    mkdir -p ../final/$i
    for j in $i/*; do
        unzip -o $j -d ../final/$i
        rm ../final/$i/*metadata*
    done
done


for i in shapefile/*; do
    mkdir -p ../final/$i
    for j in $i/*; do
        unzip $j -d ../final/${j%%.*}
        rm ../final/${j%%.*}/*metadata*
    done
done

