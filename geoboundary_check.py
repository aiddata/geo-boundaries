

import os
import fiona
from shapely.geometry import Polygon, MultiPolygon,shape
import pymongo
from pymongo import MongoClient
import zipfile
import sys
from topology_check import TopologyCheck

#ADM2:DEU, ESP,JPN,NTL,PAN,TZA, UKR,
#Assertion failed: (0), function query, file AbstractSTRtree.cpp, line 285.

#ADM3: CAN,CHN,UKR,
#ADM0: NZL


#pymongo.errors.DocumentTooLarge: BSON document too large (37090776 bytes) - the connected server supports BSON document sizes up to 16793598 bytes.

indir = sys.argv[1]

client = MongoClient('localhost', 27017)

# create testing collection in mongo database
if not "testing_collection" in client.asdf_test.collection_names():

    c_features = client.asdf_test.testing_collection
    c_features.create_index([("geometry", pymongo.GEOSPHERE)])

else:

    client.asdf_test.testing_collection.drop()


if not os.path.isdir(os.path.join(indir,'geovalid_notdb')):
    os.mkdir(os.path.join(indir,'geovalid_notdb'))
if not os.path.isdir(os.path.join(indir,'geoinvalid_notdb')):
    os.mkdir(os.path.join(indir,'geoinvalid_notdb'))
if not os.path.isdir(os.path.join(indir,'geoinvalid_db')):
    os.mkdir(os.path.join(indir,'geoinvalid_db'))
if not os.path.isdir(os.path.join(indir, 'projection_check')):
    os.mkdir(os.path.join(indir, 'projection_check'))


"""
def unzipfile(inpath):
    for root, dirs, files in os.walk(inpath):
        for name in files:
            if name.endswith('.zip'):
                filedir = os.path.join(root,name)

                try:
                    zip_ref = zipfile.ZipFile(filedir, 'r')
                    zip_ref.extractall('processed/processed/ADM0')
                    zip_ref.close()
                except:
                    print "File " + inpath + "can not be unzipped"
"""

def get_files(inpath):

    files_list = list()

    for root, dirs, files in os.walk(inpath):
        for name in files:
            if name.endswith('.shp'):
                files_list.append(os.path.join(root,name))

    return files_list


def move_file(inpath, destfolder):

    extlist = ['.shp', '.shx', '.cpg', '.dbf', '.prj', '.sbn', '.sbx', '.shp.xml']

    file_dir = os.path.dirname(inpath)
    file_nm = os.path.splitext(inpath)[0]

    for ext in extlist:

        fil = os.path.join(file_dir,file_nm+ext)

        if os.path.isfile(fil):

            destfile = os.path.join(file_dir,destfolder)

            os.rename(fil, os.path.join(destfile,os.path.basename(fil)))

def delete_file(inpath):

    extlist = ['.shp', '.shx', '.cpg', '.dbf', '.prj', '.sbn', '.sbx', '.shp.xml']

    if os.path.isfile(inpath):

        try:
            file_dir = os.path.dirname(inpath)
            file_nm = os.path.splitext(inpath)[0]

            for ext in extlist:

                fil = os.path.join(file_dir,file_nm+ext)

                if os.path.isfile(fil):

                    os.remove(fil)
        except:

            raise Exception(inpath, 'Cannot be deleted')
    else:

        raise Exception(inpath, "is not a directory")
"""
def projection_check(inpath):

    driver = ogr.GetDriverByName('ESRI Shapefile')
    dataset = driver.Open(inpath)

    # from Layer
    layer = dataset.GetLayer()
    inSpatialRef = layer.GetSpatialRef()
    sr = osr.SpatialReference(str(inSpatialRef))

    projection = sr.GetAttrValue('PROJCS')
    geogcs = str(sr.GetAttrValue('GEOGCS'))

    passlist = ['GCS_WGS_1984', 'WGS84']
    geogcs = re.sub(' ','',geogcs)

    if (projection is not None) or (geogcs not in passlist):

        print inSpatialRef
        move_file(inpath, 'projection_check')

        raise ('projection check failed for layer: ', inpath)

    else:

        print 'projection check passed for layer: ', inpath

"""

#unzipfile(samplezip)

files = get_files(indir)

for file in files:

    print "Start processing " + file

    # check projection

    try:

        t = TopologyCheck(file)
        t.projection_check()
        t.boundary_check()

    except:

        move_file(file, 'projection_check')


    if os.path.isfile(file):

        shapes = fiona.open(file)

        for feature in shapes:

            geoms = feature['geometry']

            if shape(feature['geometry']).is_valid:

                try:
                    client.asdf_test.testing_collection.insert(geoms)
                    delete_file(file)
                except:
                    move_file(file, 'geovalid_notdb')

            else:

                try:
                    client.asdf_test.testing_collection.insert(geoms)
                    move_file(file, 'geoinvalid_db')

                except:
                    move_file(file, 'geoinvalid_notdb')
    else:

        pass











