#!/bin/bash

src="${HOME}/git/geo-boundaries"
dst="/sciclone/aiddata10/REU/geoboundaries"

cp ${src}/mpi_utility.py ${dst}
cp ${src}/boundary_check.py ${dst}
cp ${src}/gb.py ${dst}
cp ${src}/jobscript ${dst}
