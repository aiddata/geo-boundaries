

# source: https://gist.github.com/benbalter/5858851

# Bulk convert shapefiles to geojson using ogr2ogr
# For more information, see http://ben.balter.com/2013/06/26/how-to-convert-shap$

# Note: Assumes you're in a folder with one or more zip files containing shape f$
# and Outputs as geojson with the crs:84 SRS (for use on GitHub or elsewhere)

# change to EPSG:4326
#geojson conversion
function shp2geojson() {
  ogr2ogr -f GeoJSON -t_srs EPSG:4326 "$1.geojson" "$1.shp"
}

#unzip all files in a directory
# change the directory to where the shapefiles are
for i in /sciclone/home10/zlv/datasets/geoboundary/shp/shp/*; do
    cd $i;

    for j in $i/*.zip; do
        unzip "$j";
        echo ${j};
        #echo ${var}
        #echo ${i%\.*}
    done

done

for i in /sciclone/home10/zlv/datasets/geoboundary/shp/shp/*; do
    cd $i;
    for var in $i/*.shp;do
       #echo ${var};
       shp2geojson ${var%\.*};
    done
done


#convert all shapefiles
#for var in *.shp; do shp2geojson ${var%\.*}; echo ${var%\.*}; done

find . -name "*.shp" -exec rm -f {} \;
find . -name "*.cpg" -exec rm -f {} \;
find . -name "*.dbf" -exec rm -f {} \;
find . -name "*.prj" -exec rm -f {} \;
find . -name "*.sbn" -exec rm -f {} \;
find . -name "*.sbx" -exec rm -f {} \;
find . -name "*.shp.xml" -exec rm -f {} \;
find . -name "*.shx.xml" -exec rm -f {} \;
find . -name "*.zip" -exec rm -f {} \;
find . -name "*.shx" -exec rm -f {} \;
