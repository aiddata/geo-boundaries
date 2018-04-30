

import sys
import os
import shutil
import zipfile
import errno
import pymongo
import pandas as pd

from boundary_check import BoundaryCheck


# -------------------------------------
# inputs

stages = "2"

version_input = (1, 3, 1)


# -------------------------------------

# make directories
def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


# prompt to continue function
def user_prompt_bool(question):
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    while True:
        sys.stdout.write(str(question) + " [y/n] \n> ")
        choice = raw_input().lower()
        if choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " +
                             "(or 'y' or "" 'n').\n")


def save_state():
    state.to_csv(state_output_path, index=False, encoding='utf-8')


# prep version
raw_version_str = "1_3"
data_version_str = "1_3_1"

# '.'.join(map(str, list(version_input)))


# # confirm version (prompt)
# confirm_version = "Confirm version input: {0}".format(version_input)
# if not user_prompt_bool(confirm_version):
#     sys.exit("Terminated: user's request.")


# # confirm stages (prompt)
# confirm_stages = "Confirm stages to run: {0}".format(', '.join(sorted(list(stages))))
# if not user_prompt_bool(confirm_stages):
#     sys.exit("Terminated: user's request.")



# mongo_server = 'localhost'
mongo_server = '128.239.108.200'

gb_dir = "/sciclone/aiddata10/REU/geoboundaries"


# input
raw_dir = os.path.join(gb_dir, "raw", raw_version_str)

metadata_path = os.path.join(raw_dir, "metadata.csv")

processed_dir = os.path.join(raw_dir, "processed")


# output
data_dir = os.path.join(gb_dir, "data", data_version_str)


# working directory
work_dir = os.path.join(gb_dir, "tmp", data_version_str)
extract_dir = os.path.join(work_dir, "extract")
updates_dir = os.path.join(work_dir, "updates")


state_output_path = os.path.join(work_dir, 'status_output.csv')


# -------------------------------------
# part 1 - initialize and extract data

if "1" in stages:
    print "Running stage 1..."

    # --------------------
    # prepare pandas table to track all actions and errors

    init_dicts = []
    adm_dirs = [i for i in os.listdir(processed_dir)
                if os.path.isdir(os.path.join(processed_dir, i))]
    for adm in adm_dirs:
        country_zips = os.listdir(os.path.join(processed_dir, adm))
        for file in country_zips:
            path = os.path.join(processed_dir, adm, file)
            parts = file.split('_')
            iso = parts[0]
            valid = len(parts) == 2 and len(iso) == 3 and parts[1].endswith('.zip') and parts[1][:4].upper() == adm.upper()
            init_dicts.append({'iso': iso.upper(), 'adm': adm.upper(), 'path': path, 'valid_init': valid})


    state = pd.DataFrame(init_dicts)


    # --------------------
    # extract

    make_dir(extract_dir)
    state['valid_extract'] = None

    # unzip all data from processed_dir to working dir
    for ix, row in state.loc[state['valid_init'] == True].iterrows():
        iso_adm = "{0}_{1}".format(row["iso"], row["adm"])
        row_dir = os.path.join(extract_dir, iso_adm)
        try:
            make_dir(row_dir)
            zip_ref = zipfile.ZipFile(row["path"], 'r')
            zip_ref.extractall(row_dir)
            zip_ref.close()
            state.at[ix, 'valid_extract'] = True
        except:
            state.at[ix, 'valid_extract'] = False

    # --------------------
    # check extracted files

    state['valid_files'] = None

    shapefile_extenions = ["shp", "shx", "dbf"]

    # unzip all data from processed_dir to working dir
    for ix, row in state.loc[state['valid_extract'] == True].iterrows():
        iso_adm = "{0}_{1}".format(row["iso"], row["adm"])
        row_dir = os.path.join(extract_dir, iso_adm)
        row_files = [os.path.join(row_dir, "{0}.{1}".format(iso_adm, ext))
                     for ext in shapefile_extenions]
        state.at[ix, 'valid_files'] = all([os.path.isfile(f) for f in row_files])
        state.at[ix, 'shapefile'] = os.path.join(row_dir, "{0}.shp".format(iso_adm))



# load previous stage 1 data as reference
# previous data must line up, any changes to underlying files will not
# be detected or revalidated
if "1" not in stages:
    print "Loading stage 1..."

    state = pd.read_csv(state_output_path, quotechar='\"',
                        na_values='', keep_default_na=False,
                        encoding='utf-8')


save_state()


# -------------------------------------
# part 2 - check data

# we need to think about how to use mongo check.
# it will throw topology errors as well as errors
# due to feature being too big for a mongo doc.
# ex:
#   pymongo.errors.DocumentTooLarge: BSON document too large (37090776 bytes) -
#   the connected server supports BSON document sizes up to 16793598 bytes.

if "2" in stages:
    print "Running stage 2..."

    make_dir(updates_dir)

    # initialize mongo connection and create test collection
    client = pymongo.MongoClient(mongo_server, 27017)
    test_db = client.geoboundaries_testing

    if 'validation' in test_db.collection_names():
        test_db.validation.drop()

    c_features = test_db.validation
    c_features.create_index([('geometry', pymongo.GEOSPHERE)])


    state['valid_proj'] = None
    state['valid_bnds'] = None
    state['valid_shapely'] = None
    state['valid_mongo'] = None

    state['error_proj'] = None
    state['error_bnds'] = None
    state['error_shapely'] = None
    state['error_mongo'] = None

    # boundary validation checks
    for ix, row in state.loc[state['valid_files'] == True].iterrows():

        print "{0} - {1} {2}".format(ix, row['iso'], row['adm'])

        bc = BoundaryCheck(row['shapefile'])

        try:
            valid, error  = bc.projection_check()
            state.at[ix, 'valid_proj'] = valid
            state.at[ix, 'error_proj'] = error
        except Exception as e:
            print e
            state.at[ix, 'valid_proj'] = False
            state.at[ix, 'error_proj'] = e

        try:
            valid, error  = bc.boundary_check()
            state.at[ix, 'valid_bnds'] = valid
            state.at[ix, 'error_bnds'] = error
        except Exception as e:
            print e
            state.at[ix, 'valid_bnds'] = False
            state.at[ix, 'error_bnds'] = e

        try:
            valid, error  = bc.shapely_check()
            state.at[ix, 'valid_shapely'] = valid
            state.at[ix, 'error_shapely'] = error
        except Exception as e:
            print e
            state.at[ix, 'valid_shapely'] = False
            state.at[ix, 'error_shapely'] = e

        try:
            valid, error = bc.mongo_check(c_features)
            state.at[ix, 'valid_mongo'] = valid
            state.at[ix, 'error_mongo'] = error
        except Exception as e:
            print e
            state.at[ix, 'valid_mongo'] = False
            state.at[ix, 'error_mongo'] = e

        bc.close()



# load previous stage 1+2 data as reference
# previous data must line up, any changes to underlying files will not
# be detected or revalidated
if "2" not in stages:
    print "Loading stage 2..."

    state = pd.read_csv(state_output_path, quotechar='\"',
                        na_values='', keep_default_na=False,
                        encoding='utf-8')


save_state()


# -------------------------------------
# part 3 - process data

# if "3" in stages:

# organize output dir
# create geojsons
# add metadata




# import json
# import fiona
# from shapely.geometry import mapping, shape


# def geojson_shape_mapping(features):
#     for feat in features:
#         feat['geometry'] = mapping(shape(feat['geometry']))
#         yield feat



# x = fiona.open("/sciclone/aiddata10/REU/geoboundaries/tmp/1_3_1/extract/AFG_ADM1/AFG_ADM1.shp")

# features = list(geojson_shape_mapping(x))


# geojson_out = {
#     "type": "FeatureCollection",
#     "features": features
# }

# with open(path, "w") as f:
#     f.write(json.dumps(geojson_out))


# save_state()


