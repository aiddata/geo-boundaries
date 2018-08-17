'''


at each stage:
# load previous stages data
# - used all the time to load master state on workers, as well as when
#   previous state is not being run
# - previous data must line up, any changes to underlying files will not
#   be detected or revalidated


'''

import sys
import os
import shutil
import zipfile
import json
import errno
import datetime
import time
from copy import deepcopy

import fiona
import pandas as pd
import geopandas as gpd
from shapely.geometry import mapping, shape

from boundary_check import BoundaryCheck

import mpi_utility


parallel = True

if parallel:

    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    if rank == 0:
        print "Running in parallel mode ({} cores)...".format(size)

else:

    print "Running in serial mode..."

    size = 1
    rank = 0



if rank == 0:
    builder_time = int(time.time())
    builder_start = time.localtime()
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', builder_start)



# -------------------------------------
# inputs
# static for now - could be script args later

stages = "346"

version_input = (1, 3, 3)

field_lookup = {
    "raw_file_name": "Processed File Name",
    "country": "Group"
}


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
    if parallel: comm.Barrier()

    if rank == 0:
        state.to_csv(state_output_path, index=False, encoding='utf-8')

    if parallel: comm.Barrier()


def geojson_shape_mapping(features):
    for feat in features:
        feat['geometry'] = mapping(shape(feat['geometry']))
        yield feat


# prep version
raw_version_str = "1_3"
data_version_str = "1_3_3"

# '.'.join(map(str, list(version_input)))


# # confirm version (prompt)
# confirm_version = "Confirm version input: {0}".format(version_input)
# if not user_prompt_bool(confirm_version):
#     sys.exit("Terminated: user's request.")


# # confirm stages (prompt)
# confirm_stages = "Confirm stages to run: {0}".format(', '.join(sorted(list(stages))))
# if not user_prompt_bool(confirm_stages):
#     sys.exit("Terminated: user's request.")


use_mongo = True

# mongo_server = 'localhost'
mongo_server = '128.239.108.200'

gb_dir = "/sciclone/aiddata10/REU/geoboundaries"


# input
raw_dir = os.path.join(gb_dir, "raw", raw_version_str)

metadata_path = os.path.join(raw_dir, "metadata.csv")

processed_dir = os.path.join(raw_dir, "processed")


# output
data_dir = os.path.join(gb_dir, "data", data_version_str)

final_dir = os.path.join(data_dir, "final")

metadata_dir = os.path.join(final_dir, "metadata")
make_dir(metadata_dir)

zip_dir = os.path.join(data_dir, "zip")


state_output_path = os.path.join(data_dir, 'status_output.csv')


# working directory
work_dir = os.path.join(gb_dir, "tmp", data_version_str)
extract_dir = os.path.join(work_dir, "extract")


state = None


# -------------------------------------
# stage 1 - initialize and extract data


if "1" in stages:
    if rank == 0:
        print "Running stage 1..."

    # --------------------
    # prepare pandas table to track all actions and errors

    init_dicts = []
    adm_dirs = [i for i in os.listdir(processed_dir)
                if os.path.isdir(os.path.join(processed_dir, i))]
    for adm in adm_dirs:
        country_zips = os.listdir(os.path.join(processed_dir, adm))
        for file in country_zips:
            if file.endswith(".DS_Store"):
                continue
            path = os.path.join(processed_dir, adm, file)
            parts = file.split('_')
            iso = parts[0]
            valid = len(parts) == 2 and len(iso) == 3 and parts[1].endswith('.zip') and parts[1][:4].upper() == adm.upper()
            init_dicts.append({'iso': iso.upper(), 'adm': adm.upper(), 'path': path, 'valid_init': valid})


    state = pd.DataFrame(init_dicts)


    def s1_general_init(self):
        pass

    def s1_master_init(self):
        # start job timer
        self.Ts = int(time.time())
        self.T_start = time.localtime()
        print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)

        self.state['valid_extract'] = None
        self.state['valid_files'] = None
        self.state['shapefile'] = None


    def s1_worker_job(self, task_index, task_data):

        ix, row = task_data

        iso_adm = "{0}_{1}".format(row["iso"], row["adm"])
        row_dir = os.path.join(extract_dir, iso_adm)


        try:
            make_dir(row_dir)

            # extract files
            zip_ref = zipfile.ZipFile(row["path"], 'r')
            zip_ref.extractall(row_dir)
            zip_ref.close()
            valid_extract = True

            # make sure all shapefiles exist
            row_files = [os.path.join(row_dir, "{0}.{1}".format(iso_adm, ext))
                         for ext in self.shapefile_extenions]
            valid_files = all([os.path.isfile(f) for f in row_files])

            # primary shapefile path
            valid_shapefile_path = os.path.join(row_dir, "{0}.shp".format(iso_adm))

        except Exception as e:
            print e
            valid_extract = False
            valid_files = None
            valid_shapefile_path = None

        return (ix, valid_extract, valid_files, valid_shapefile_path)


    def s1_master_process(self, worker_result):
        ix, valid_extract, valid_files, valid_shapefile_path = worker_result
        self.state.at[ix, 'valid_extract'] = valid_extract
        self.state.at[ix, 'valid_files'] = valid_files
        self.state.at[ix, 'shapefile'] = valid_shapefile_path


    def s1_master_final(self):
        # stop job timer
        T_run = int(time.time() - self.Ts)
        T_end = time.localtime()
        print '\n\n'
        print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)
        print 'End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end)
        print 'Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'
        print '\n\n'


    s1_job = mpi_utility.NewParallel(parallel=parallel)

    s1_job.state = state.copy(deep=True)
    s1_job.shapefile_extenions = ["shp", "shx", "dbf"]

    s1_qlist = list(state.loc[state['valid_init'] == True].iterrows())

    s1_job.set_task_list(s1_qlist)
    s1_job.set_general_init(s1_general_init)
    s1_job.set_master_init(s1_master_init)
    s1_job.set_worker_job(s1_worker_job)
    s1_job.set_master_process(s1_master_process)
    s1_job.set_master_final(s1_master_final)

    s1_job.run()

    if rank == 0:
        state = s1_job.state.copy(deep=True)

    save_state()



state = pd.read_csv(state_output_path, quotechar='\"',
                    na_values='', keep_default_na=False,
                    encoding='utf-8')


if parallel: comm.Barrier()


# -------------------------------------
# stage 2 - check data

# we need to think about how to use mongo check.
# it will throw topology errors as well as errors
# due to feature being too big for a mongo doc.
# ex:
#   pymongo.errors.DocumentTooLarge: BSON document too large (37090776 bytes) -
#   the connected server supports BSON document sizes up to 16793598 bytes.

if "2" in stages:

    if rank == 0:
        print "Running stage 2..."

    c_features = None
    if use_mongo:
        import pymongo

        # initialize mongo connection and create test collection
        client = pymongo.MongoClient(mongo_server, 27017)
        test_db = client.geoboundaries_testing


        if rank == 0 and 'validation' in test_db.collection_names():
            test_db.validation.drop()
            c_features = test_db.validation
            c_features.create_index([('geometry', pymongo.GEOSPHERE)])

        c_features = test_db.validation


    def s2_general_init(self):
        pass


    def s2_master_init(self):
        # start job timer
        self.Ts = int(time.time())
        self.T_start = time.localtime()
        print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)

        self.state['valid_proj'] = None
        self.state['valid_bnds'] = None
        self.state['valid_shapely'] = None
        self.state['valid_mongo'] = None

        self.state['error_proj'] = None
        self.state['error_bnds'] = None
        self.state['error_shapely'] = None
        self.state['error_mongo'] = None


    def s2_worker_job(self, task_index, task_data):

        ix, row = task_data

        bc = BoundaryCheck(row['shapefile'])

        try:
            valid_proj, error_proj = bc.projection_check()
        except Exception as e:
            valid_proj, error_proj = False, e

        try:
            valid_bnds, error_bnds = bc.boundary_check()
        except Exception as e:
            valid_bnds, error_bnds = False, e

        try:
            valid_shapely, error_shapely = bc.shapely_check()
        except Exception as e:
            valid_shapely, error_shapely = False, e

        valid_mongo, error_mongo = None, None
        if self.use_mongo:
            try:
                valid_mongo, error_mongo = bc.mongo_check(self.c_features)
            except Exception as e:
                valid_mongo, error_mongo = False, e

        bc.close()

        return (ix, valid_proj, error_proj, valid_bnds, error_bnds, valid_shapely, error_shapely, valid_mongo, error_mongo)


    def s2_master_process(self, worker_result):
        ix, valid_proj, error_proj, valid_bnds, error_bnds, valid_shapely, error_shapely, valid_mongo, error_mongo = worker_result

        self.state.at[ix, 'valid_proj'] = valid_proj
        self.state.at[ix, 'error_proj'] = error_proj

        self.state.at[ix, 'valid_bnds'] = valid_bnds
        self.state.at[ix, 'error_bnds'] = error_bnds

        self.state.at[ix, 'valid_shapely'] = valid_shapely
        self.state.at[ix, 'error_shapely'] = error_shapely

        self.state.at[ix, 'valid_mongo'] = valid_mongo
        self.state.at[ix, 'error_mongo'] = error_mongo


    def s2_master_final(self):
        # stop job timer
        T_run = int(time.time() - self.Ts)
        T_end = time.localtime()
        print '\n\n'
        print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)
        print 'End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end)
        print 'Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'
        print '\n\n'


    s2_job = mpi_utility.NewParallel(parallel=parallel)

    s2_job.state = state.copy(deep=True)
    s2_job.use_mongo = use_mongo
    s2_job.c_features = c_features

    s2_qlist = list(state.loc[state['valid_files'] == True].iterrows())

    s2_job.set_task_list(s2_qlist)
    s2_job.set_general_init(s2_general_init)
    s2_job.set_master_init(s2_master_init)
    s2_job.set_worker_job(s2_worker_job)
    s2_job.set_master_process(s2_master_process)
    s2_job.set_master_final(s2_master_final)

    s2_job.run()

    if rank == 0:
        state = s2_job.state.copy(deep=True)

    save_state()



state = pd.read_csv(state_output_path, quotechar='\"',
                    na_values='', keep_default_na=False,
                    encoding='utf-8')

if parallel: comm.Barrier()



# -------------------------------------
# stage 3 - process metadata

from unidecode import unidecode

if "3" in stages and rank == 0:

    print "Running stage 3..."

    # load metadata
    full_metadata_src = pd.read_csv(metadata_path, quotechar='\"',
                                    na_values='', keep_default_na=False,
                                    encoding='utf-8')

    state['metadata'] = None
    state['metadata_error'] = None


    # could change this to use only rows without any errors across all stages
    # or anything else, as needed

    for ix, row in state.loc[state['valid_files'] == True].iterrows():
    # for ix, row in state.loc[state['iso'].isin(["COD", "FSM"])].iterrows():


        print "{0} - {1} {2}".format(ix, row['iso'], row['adm'])

        # lookup metadata
        metadata_src = full_metadata_src.loc[full_metadata_src[field_lookup["raw_file_name"]] == "{0}_{1}.zip".format(row["iso"], row["adm"])]

        # make sure we have one metadata entry
        n_metadata = len(metadata_src)
        if n_metadata > 1:
            state.at[ix, 'metadata'] = False
            state.at[ix, 'metadata_error'] = "Too many metadata matches ({0})".format(n_metadata)
            continue
        elif n_metadata == 0:
            state.at[ix, 'metadata'] = False
            state.at[ix, 'metadata_error'] = "Missing metadata"
            continue

        # create metadata JSON
        metadata = json.loads(metadata_src.to_json(orient="records"))[0]


        metadata["country"] = metadata[field_lookup["country"]]
        metadata["adm"] = row["adm"]
        metadata["iso"] = row["iso"]
        metadata["version"] = data_version_str

        metadata["timestamp"] = int(time.time())
        metadata["datetime"] = datetime.datetime.fromtimestamp(metadata["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')


        iso_adm = "{0}_{1}".format(row["iso"], row["adm"])

        metadata_out_path = os.path.join(metadata_dir, "{}.json".format(iso_adm))

        with open(metadata_out_path, "w") as f:
            json.dump(metadata, f, indent=4)
            f.write("\n")

        state.at[ix, 'metadata'] = True


if "3" in stages:
    save_state()


state = pd.read_csv(state_output_path, quotechar='\"',
                    na_values='', keep_default_na=False,
                    encoding='utf-8')

if parallel: comm.Barrier()



# -------------------------------------
# stage 4 - output data

# can add other formats here if needed in future.
# can specify only some format to build if needed.
make_shapefile = True
make_geojson = True

make_geojson_simple = True
simplify_tolerance = 0.01

if "4" in stages:

    if rank == 0:
        print "Running stage 4..."


    qlist = list(state.loc[state['metadata'] == True].index)

    c = deepcopy(rank)

    while c < len(qlist):

        ix = qlist[c]
        row = state.iloc[ix]

        c += size

        print "{0} - {1} {2}".format(ix, row['iso'], row['adm'])

        iso_adm = "{0}_{1}".format(row["iso"], row["adm"])

        metadata_out_path = os.path.join(metadata_dir, "{}.json".format(iso_adm))

        final_geojson_path = os.path.join(final_dir, "geojson", row["iso"],
                                          "{}.geojson".format(iso_adm))

        final_geojson_simple_path = os.path.join(final_dir, "geojson_simple", row["iso"],
                                                 "{}_simple.geojson".format(iso_adm))

        final_shapefile_dir = os.path.join(final_dir, "shapefile", row["iso"], iso_adm)

        final_shapefile_path = os.path.join(final_shapefile_dir, "{}.shp".format(iso_adm))


        make_dir(os.path.dirname(final_geojson_path))
        make_dir(os.path.dirname(final_shapefile_path))
        make_dir(os.path.dirname(final_geojson_simple_path))



        zip_geojson_path = os.path.join(zip_dir, "geojson", row["iso"],
                                        "{}.geojson.zip".format(iso_adm))

        zip_shapefile_path = os.path.join(zip_dir, "shapefile", row["iso"],
                                          "{}.shp.zip".format(iso_adm))

        zip_geojson_simple_path = os.path.join(zip_dir, "geojson_simple", row["iso"],
                                               "{}_simple.geojson.zip".format(iso_adm))


        make_dir(os.path.dirname(zip_geojson_path))
        make_dir(os.path.dirname(zip_shapefile_path))
        make_dir(os.path.dirname(zip_geojson_simple_path))


        # --------------------
        # convert shapefile to GeoJSON first
        # also fix simple errors detected earilier

        raw_shapefile_path = state.at[ix, 'shapefile']

        shps = fiona.open(raw_shapefile_path)

        features = list(geojson_shape_mapping(shps))

        shps.close()


        id_template = "{0}_{1}_{2}".format(row["iso"], row["adm"], data_version_str)
        unique_id_field = "gbid"

        for i, _ in enumerate(features):
            features[i]["properties"]["iso"] = row["iso"]
            features[i]["properties"]["adm"] = row["adm"]
            features[i]["properties"]["adm_int"] = int(row["adm"][3:])
            features[i]["properties"]["feature_id"] = str(i)
            features[i]["properties"][unique_id_field] = "{0}_{1}".format(id_template, i)
            # fix simple errors
            if row['error_shapely'] == "fixable":
                fgeom = shape(features[i]['geometry'])
                if not fgeom.is_valid:
                    features[i]['geometry'] = mapping(fgeom.buffer(0))


        geojson_out = {
            "type": "FeatureCollection",
            "features": features
        }

        with open(final_geojson_path, "w") as f:
            json.dump(geojson_out, f)
            f.write("\n")


        # --------------------


        if make_shapefile:

            gdf = gpd.read_file(final_geojson_path)
            gdf.to_file(filename=final_shapefile_path)

            shp_files = [f for f in os.listdir(final_shapefile_dir) if not os.path.isdir(os.path.join(final_shapefile_dir, f))]

            with zipfile.ZipFile(zip_shapefile_path, 'w') as myzip:
                myzip.write(metadata_out_path, "metadata.json")
                for f in shp_files:
                    myzip.write(os.path.join(final_shapefile_dir, f), f)


        if make_geojson:

            with zipfile.ZipFile(zip_geojson_path, 'w') as myzip:
                myzip.write(final_geojson_path, "{}.geojson".format(iso_adm))
                myzip.write(metadata_out_path, "metadata.json")


        if make_geojson_simple:

            gdf = gpd.read_file(final_geojson_path)

            gdf['geometry'] = gdf['geometry'].simplify(simplify_tolerance)

            with open(final_geojson_simple_path, "w", 0) as f:
                json.dump(json.loads(gdf.to_json()), f)
                f.write("\n")

            with zipfile.ZipFile(zip_geojson_simple_path, 'w') as myzip:
                myzip.write(final_geojson_simple_path, "{}_simple.geojson".format(iso_adm))
                myzip.write(metadata_out_path, "metadata.json")



    save_state()



state = pd.read_csv(state_output_path, quotechar='\"',
                    na_values='', keep_default_na=False,
                    encoding='utf-8')


if parallel: comm.Barrier()


# -------------------------------------
# stage 5 - cleanup tmp data


if "5" in stages and rank == 0:

    print "Running stage 5..."

    # clean up tmp files
    os.rmtree(work_dir)


if parallel: comm.Barrier()


# -------------------------------------
# stage 6 - move to geoquery dir


if "6" in stages:

    if rank == 0:
        print "Running stage 6..."

    geoquery_dir = "/sciclone/aiddata10/REU/geo/data/boundaries/geoboundaries/{}".format(data_version_str)

    qlist = list(state.loc[state['metadata'] == True].index)

    c = deepcopy(rank)

    while c < len(qlist):

        ix = qlist[c]
        row = state.iloc[ix]

        c += size

        print "{0} - {1} {2}".format(ix, row['iso'], row['adm'])

        iso_adm = "{0}_{1}".format(row["iso"], row["adm"])

        metadata_out_path = os.path.join(metadata_dir, "{}.json".format(iso_adm))

        final_geojson_path = os.path.join(final_dir, "geojson", row["iso"],
                                          "{}.geojson".format(iso_adm))

        row_dir = os.path.join(geoquery_dir, iso_adm)
        make_dir(row_dir)

        geoquery_metadata_path = os.path.join(row_dir, "metadata.json")
        geoquery_geojson_path = os.path.join(row_dir, os.path.basename(final_geojson_path))

        shutil.copy(final_geojson_path, geoquery_geojson_path)
        shutil.copy(metadata_out_path, geoquery_metadata_path)





# -------------------------------------

if rank == 0:
    builder_run = int(time.time() - builder_time)
    print '\n\n'
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', builder_start)
    print 'End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', time.localtime())
    print 'Runtime: ' + str(builder_run//60) +'m '+ str(int(builder_run%60)) +'s'
    print '\n\n'
