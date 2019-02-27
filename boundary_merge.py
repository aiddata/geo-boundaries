
"""
This script is used to merge multiple administrative boundary geojson files within a directory

Example to run:
    python boundary_merge.py ADM1 (1 or adm1 or Adn1) /sciclone/aiddata10/REU/geo/data/boundaries/geoboundaries/1_3_3/ /sciclone/data10/zlv/data_process/geoboundary_merge/geoboundary_global_ADM1.json_


* This script is modified based on: https://gist.github.com/migurski/3759608

"""

import os
from json import load, JSONEncoder
from optparse import OptionParser
from re import compile




float_pat = compile(r'^-?\d+\.\d+(e-?\d+)?$')
charfloat_pat = compile(r'^[\[,\,]-?\d+\.\d+(e-?\d+)?$')

parser = OptionParser(usage="""%prog [options]

Group the same administrative level GeoJson files under a given directory into one output file.

Example:
  python %prog -p 2 ADM1 (or 1 or adm1) ~/Documents/mirandalv/GeoBoundaries ~/Documents/mirandalv/GeoBoundaries/output.json""")

defaults = dict(precision=6)

parser.set_defaults(**defaults)

parser.add_option('-p', '--precision', dest='precision',
                  type='int', help='Digits of precision, default %(precision)d.' % defaults)



if __name__ == '__main__':

    options, args = parser.parse_args()
    adm_level, infiles_dir, outfile = args[0], args[1], args[-1]


    # checking adms

    if adm_level.isdigit():

        adm_level = "ADM" + adm_level

    elif isinstance(adm_level, basestring) and len(adm_level)==4 and adm_level[:-1].upper() == 'ADM':

        adm_level = adm_level.upper()

    else:

        raise Exception('Sorry, "%s" does not look like correct administrative boundary level' % adm_level)


    infiles = list()

    # create a list of ADM file path
    for file in os.listdir(infiles_dir):

        if adm_level in file:

            jsonfile = os.path.join(infiles_dir, file, file + '.geojson')

            if os.path.isfile(jsonfile):

                infiles.append(jsonfile)


    outjson = dict(type='FeatureCollection', features=[])


    for infile in infiles:
        injson = load(open(infile))

        if injson.get('type', None) != 'FeatureCollection':
            raise Exception('Sorry, "%s" does not look like GeoJSON' % infile)

        if type(injson.get('features', None)) != list:
            raise Exception('Sorry, "%s" does not look like GeoJSON' % infile)

        outjson['features'] += injson['features']

    encoder = JSONEncoder(separators=(',', ':'))
    encoded = encoder.iterencode(outjson)

    format = '%.' + str(options.precision) + 'f'
    output = open(outfile, 'w')

    for token in encoded:
        if charfloat_pat.match(token):
            # in python 2.7, we see a character followed by a float literal
            output.write(token[0] + format % float(token[1:]))

        elif float_pat.match(token):
            # in python 2.6, we see a simple float literal
            output.write(format % float(token))

        else:
            output.write(token)

