# sparcgis
Use Koalas dataframes for your distributed geospatial datasets with convenient interoperability with Esri datatypes and ArcGIS software.

## About
This is a repository with some utilities for interoperability between Spark and ArcGIS (hence sparcgis). Specifically, there are utilities for converting Koalas DataFrames to ArcGIS data types - Feature Collections, Feature Layers, Feature Sets, and Feature Classes.

[Koalas](https://github.com/databricks/koalas) is a python library that wraps the pandas API around Spark, reducing the barrier to entry for big data analytics for data analysts/scientists used to working with pandas in-memory datasets.

As of version 1.10, Koalas supports API extensions, implemented by yours truly with inspiration from @achapkowski and the pandas.api.extensions module (see [Docs](https://koalas.readthedocs.io/en/latest/reference/extensions.html), [Release Notes](https://github.com/databricks/koalas/releases/tag/v1.1.0), and [PR #1617](https://github.com/databricks/koalas/pull/1617)) for details).

This library takes advantage of that new functionality to extend Koalas dataframes with spatial datatypes and integration capabilities specific to ArcGIS.

## Status
This repo is in early development and shouldn't be used for production environments. At this stage, this repository merely demonstrates the potential of Koalas integration with the [ArcGIS API for Python](https://developers.arcgis.com/python/). 

With that said, there's the skeleton of a usable Koalas GeoAccessor here and a roadmap to implementing more powerful functionality. You'll find a lot of functions are `NotImplemented` and will error out as such. Once these are written, sparcgis will provide a minimal API for interoperability.

## Usage
Example usage of currently implemented functionality:
```
import databricks.koalas as ks
from arcgis.geometry import Point
from sparcgis.koalas import KoalasGeoAccessor

kdf = ks.DataFrame({'x': [1.,2.,3.,4.,5.], 'y': [1.,2.,3.,4.,5.]})
kdf.spatial.sr(3857).geometry(Point) # designate geometry type and spatial reference

fset = kdf.spatial.to_dict() # convert to dict representation of a FeatureSet
```

Eventually, the library will support non-point geometries, basic spatial aggregations, and ArcGIS Online/Enteprise publishing capabilities.

## License
Apache 2.0 @ Samuel Cook, 2020

Note: while sparcgis is free and open source software, it is built on the ArcGIS API for Python which is free but licensed via the [Esri Master License Agreement](https://www.esri.com/content/dam/esrisites/en-us/media/legal/ma-translations/english.pdf), and sparcgis is intended to be used in compliance with this license. The sparcgis library in no way indicates that the ArcGIS API for Python is, should, or must be distributed as open source software. Moreover, it is the responsibility of the sparcgis end user to ensure their compliance with Esri's Master License Agreement.