
import os
from os import listdir
from os.path import isfile, join
import shutil

mypath = r"/sciclone/home10/zlv/datasets/testing_delete/geojson/shp"
outpath = r"/sciclone/home10/zlv/datasets/testing_delete/geojson/shp"

#mypath = r"/"
#outpath = r"/sciclone/home10/zlv/datasets/geoboundary/shp/shp"


for root, dirs, files in os.walk(mypath):
    for file in files:

        country = file[0:3]
        newfolder = file[0:8]


        if not os.path.exists(os.path.join(root,newfolder)):
            os.mkdir(os.path.join(root,newfolder))
            #copyfile(os.path.join(root,file), os.path.join(outpath,country))
            shutil.move(os.path.join(root,file), os.path.join(root,newfolder))
        else:
            shutil.move(os.path.join(root, file), os.path.join(root, newfolder))


