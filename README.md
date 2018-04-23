# geo-boundaries

- topology check (add later)
- metadata generator (add later)
- run geoboundary_organizer.py
    - change the input directory and output directory before running geoboundary_organizer.py
- open terminal and change directory to where the output directory is, and bash zipshp.sh to zip all shapefiles to separated folders
- run shp2geojson.sh to convert shapefiles to geojson
- run organize_geojson.py to make all geojson files to different subfolders
- rerun zipshp.sh to zip all geojson files