



import os
from os import listdir
from os.path import isfile, join
import shutil



#mypath = r"/Users/miranda/Documents/ForMiranda/ForMiranda2/processed/processed"
#outpath = r"/Users/miranda/Documents/ForMiranda/ForMiranda2/processed/organized"

# this is to reorganize the files by country
#mypath = r"/Users/miranda/Documents/ForMiranda/ForMiranda2/shp"
#outpath = r"/Users/miranda/Documents/ForMiranda/ForMiranda2/shp"

mypath = r"/sciclone/home10/zlv/datasets/geoboundary/shp"
outpath = r"/sciclone/home10/zlv/datasets/geoboundary/shp"

for root, dirs, files in os.walk(mypath):
    for file in files:
        if file.endswith(".zip"):
            country = file[0:3]
            if not os.path.exists(os.path.join(outpath,country)):
                os.mkdir(os.path.join(outpath,country))
                #copyfile(os.path.join(root,file), os.path.join(outpath,country))
                shutil.move(os.path.join(root,file), os.path.join(outpath,country))
            else:
                shutil.move(os.path.join(root, file), os.path.join(outpath, country))



