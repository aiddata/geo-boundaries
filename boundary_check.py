
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

        return valid


    def boundary_check(self):
        xmin, ymin, xmax, ymax = self.shps.bounds
        valid = (xmin >= -180) and (xmax <= 180) and (ymin >= -90) and (ymax <= 90)
        return valid


    def shapely_check(self):
        valid = True
        for feature in self.shps:
            valid = shape(feature['geometry']).is_valid
            if not valid:
                break
        return valid


    def mongo_check(self, c_features):
        valid = True
        for feature in self.shps:
            geom = feature['geometry']
            try:
                c_features.insert(geom)
            except:
                valid = False
                break
        return valid


