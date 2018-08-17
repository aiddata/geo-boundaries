

GeoBoundaries Readme


-------------------
Versioning

GeoBoundaries is versioned using a modified version of the major.minor.patch notation.
The **major** version number is bumped based on when a new ingest of GeoBoundaries into GeoQuery occurs.
For example, if the current version of GeoBoundaries is 1.3.0 and this version is ingested into GeoQuery,
the next version will be 2.0.0

**Minor** versions indicate that the raw data may have changes and is bumped any time a new download from Google
Drive occurs. For example, if GeoQuery is using GeoBoundaries version 1.3.0, and the current GeoBoundaries is 2.0.0,
then the next time GeoBoundaries data is updated without updating GeoQuery the version of GeoBoundaries
will be bumped to 2.1.0

The **patch** version is to indicate processing changes that occur when the underlying data does not change.
These would typically be associated with bug fixes or other small format changes. For example, if the formatting
of a text field in the metadata was changed, version 2.1.0 would be bumped to version 2.1.1

Note: GeoBoundaries does not make assurances of backwards compatibility with earlier versions based on
versioning (which is commonly indicated by major version number when dealing with code). That said, we will attempt
to include the scope of all changes with each version bump in the change log, and any changes which impact
backwards compatibility will be clearly indicated.



-------------------
Structure


__Raw__

The `raw` directory contains versions of GeoBoundaries in their initial form, downloaded from Google Drive, without modification.
Each subdirectory is identified by only major and minor version numbers since each time a new download occurs the minor version is bumped.

Example subdirectory: `1_3` would be used for all versions 1.3.x

Subdirectories include:

- the processed data downloaded directly from Google Drive as zip file(s)
- the extracted version of above zip in the `processed` directory
- the `metadata` csv pulled from the "Processed Data" tab of the geoquery > geoboundaries > geoboundaries_resources > GeoBoundaries Dataset Tracking and Processing sheet on Google Drive


__Tmp__

The `tmp` directory contains temporary files produced by the GeoBoundaries builder


__Data__

The `data` directory contains the fully versioned final outputs, in their final uncompressed form as well as compressed form (zip). These are produced by processing a corresponding version of data from the `raw` directory. Metadata is contained within each zip file as well as separately in the `final/metadata` dir.

Example subdirectory: `1_3_1` would indicate that the `raw/1_3` data was used with modifications to the processing initially used on version 1.3.0

Subdirectories for both `final` (uncompressed) and `zip` (compressed) data include subdirectory structures for multiple file formats (geojson, shapefile, etc.) based on the options used to run the builder.

An example path is `zip/geojson/AFG/AFG_ADM1.zip`


-------------------
Processing


See:
https://github.com/itpir/geo-boundaries




