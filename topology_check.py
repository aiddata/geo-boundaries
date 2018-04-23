
from osgeo import ogr, osr
import re
import fiona

class TopologyCheck:

    def __init__(self, path):

        self.path = path
        driver = ogr.GetDriverByName('ESRI Shapefile')
        self.dataset = driver.Open(self.path)


    def projection_check(self):

        # from Layer
        layer = self.dataset.GetLayer()
        inSpatialRef = layer.GetSpatialRef()
        sr = osr.SpatialReference(str(inSpatialRef))

        projection = sr.GetAttrValue('PROJCS')
        geogcs = sr.GetAttrValue('GEOGCS')

        passlist = ['GCS_WGS_1984', 'WGS84']
        geogcs = re.sub(' ', '', geogcs)

        if (projection is not None) or (geogcs not in passlist):

            raise Exception('The projection check failed for layer: ', self.path)

        else:

            print 'The projection check passed for layer: ', self.path


    def boundary_check(self):

        shps = fiona.open(self.path)
        xmin, ymin, xmax, ymax = shps.bounds

        try:

            if (xmin >= -90) or (xmax <= 90) or (ymin >= -180) or (ymax <= 180):

                print "The boundary check passed for {}".format(self.path)

        except ValueError:

            ('The {} is output the boundary check'.format(self.path))





