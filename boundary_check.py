
import os
import errno
import re
import shutil
from copy import deepcopy
import fiona
from shapely.geometry import mapping, shape
from osgeo import ogr, osr


class BoundaryCheck:

    def __init__(self, path):
        self.path = path
        src = fiona.open(self.path)
        self.src_driver = src.driver
        self.src_crs = src.crs
        self.src_schema = src.schema
        # self.shps = deepcopy(src)
        self.shps = src


    def make_dir(self, path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise


    def projection_check(self):
        error = None
        driver = ogr.GetDriverByName('ESRI Shapefile')
        dataset = driver.Open(self.path)

        layer = dataset.GetLayer()
        inSpatialRef = layer.GetSpatialRef()
        sr = osr.SpatialReference(str(inSpatialRef))

        # should be `None` when WGS84/EPSG:4326
        projection = sr.GetAttrValue('PROJCS')

        geogcs = sr.GetAttrValue('GEOGCS')
        geogcs = re.sub(' ', '', geogcs)

        geogcs_passlist = ['GCS_WGS_1984', 'WGS84']

        valid = (projection is None) and (geogcs in geogcs_passlist)
        if not valid:
            if projection is not None and geogcs not in geogcs_passlist:
                error = "geogcs: {0}, projection: {1}".format(geogcs, projection)
            elif projection is not None:
                error = "projection: {0}".format(projection)
            else:
                error = "geogcs: {0}".format(geogcs)
        return valid, error


    def boundary_check(self):
        error = None
        # tolerance
        tol = 1e-12
        xmin, ymin, xmax, ymax = self.shps.bounds
        valid = (xmin >= -180-tol) and (xmax <= 180+tol) and (ymin >= -90-tol) and (ymax <= 90+tol)
        if not valid:
            error = "xmin: {0}, xmax: {1}, ymin: {2}, ymax: {3}".format(xmin, xmax, ymin, ymax)
        return valid, error


    def _save_shapely_fixes(self, shapes):
        fix_path = self.path.replace("extract", "fixed")
        fix_dir = os.path.dirname(fix_path)
        self.make_dir(fix_dir)
        with fiona.open(fix_path, 'w', driver=self.src_driver, crs=self.src_crs, schema=self.src_schema) as c:
            c.writerecords(shapes)
        shutil.make_archive(fix_dir, "zip", fix_dir)
        shutil.rmtree(fix_dir)


    def shapely_check(self):
        valid = True
        error = None
        fixed = []
        for feature in self.shps:
            raw_shape = shape(feature['geometry'])
            valid = raw_shape.is_valid
            if valid:
                fixed.append(feature)
            if not valid:
                fixed_shape = raw_shape.buffer(0)
                fix_valid = fixed_shape.is_valid
                if fix_valid and error is None:
                    error = "fixable"
                    feature["geometry"] = mapping(fixed_shape)
                    fixed.append(feature)
                elif not fix_valid:
                    if error is not None:
                        error = "partial"
                    else:
                        error = "failed"
                    break
        if error == "fixable":
            self._save_shapely_fixes(fixed)
        return valid, error


    def mongo_check(self, c_features):
        valid = True
        error = None
        for feature in self.shps:
            geom = feature['geometry']
            try:
                c_features.insert(geom)
            except Exception as e:
                error = e
                valid = False
                break
        return valid, error
