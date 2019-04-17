
import re
import fiona
from shapely.geometry import shape
from osgeo import ogr, osr



class BoundaryCheck:

    def __init__(self, path):
        self.path = path
        self.shps = fiona.open(self.path)


    def close(self):
        self.shps.close()

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


    def shapely_check(self):
        valid = True
        error = None
        for feature in self.shps:
            valid = shape(feature['geometry']).is_valid
            if not valid:
                fix_valid = shape(feature['geometry']).buffer(0).is_valid
                if fix_valid and error is None:
                    error = "fixable"
                elif not fix_valid:
                    if error is not None:
                        error = "partial"
                    break
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
