import datetime

import numpy as np
import pandas as pd

import arcgis
from arcgis.features import (
    Feature,
    FeatureSet,
    FeatureLayer,
    FeatureCollection,
    FeatureLayerCollection,
)
from arcgis.geometry import (
    Point,
    MultiPoint,
    Polyline,
    Polygon,
    SpatialReference,
)

import databricks.koalas as ks
from databricks.koalas.extensions import (
    register_dataframe_accessor,
    register_series_accessor,
    register_index_accessor,
)


@register_dataframe_accessor("spatial")
class KoalasGeoAccessor:
    """
    Spatially enable your Koalas DataFrame using the ArcGIS API for Python - example:
    >> from arcgis.features import Point
    >> from sparcgis.koalas import KoalasGeoAccessor
    >> kdf['x'] = longitude_data
    >> kdf['y'] = latitudate_data
    >> kdf.spatial.sr(3857).geometry(Point).to_featurelayer(title='my_agol_layer', gis=gis) # not yet implemented
    >> fset_dict = kdf.spatial.sr(4326).geometry(Point).to_dict()
    """

    # key differences between KoalasGeoAccessor and arcgis.features.GeoAccessor:
    # 1) specifying instead of discovering geometry types and metadata
    # 2) DF-level metadata storage (i.e. there's now row-level insertion of esri geometry/metadata)
    # 3) no nested datatypes - not supported by Koalas yet, meaning it loses convenient SHAPE column behavior
    # #3 explains why the first two differences exist - with the pandas GeoAccessor, we can have a geometry
    # column that nests the information we need about a feature into a single dictionary, making geometry and metadata 
    # discovery a simple task, but without it, every column has the potential to hold relevant geographic information
    # and in a much less structured format

    def __init__(self, obj):
        self.obj = obj
        self.index = obj.index
        self._name = None
        self.geom_type = None
        self.spatial_reference = None

    def __feature_set__(self):
        """
        Returns a dict representation of a FeatureSet for self.obj
        """
        if self.geom_type is None:
            raise ValueError(
                "Geometry type must be specified as one of ",
                " [Point, Multipoint, Polyline, Polygon]: set with df.spatial.geometry(geom_type)",
            )

        cols_norm = list(self.obj.columns)
        cols_lower = [col.lower() for col in self.obj.columns]
        fields, features, date_fields = [], [], []

        if self.spatial_reference is None:
            self.sr()

        fset = {
            "objectIdFieldName": "",
            "globalIdFieldName": "",
            "displayFieldName": "",
            "geometryType": self.geom_type,
            "fields": [],
            "features": [],
        }

        sub = self.obj.select_dtypes(include=[float, int, complex])
        self.obj[sub.columns.to_list()] = sub.fillna(0)

        # create esri fields for each column in the dataframe
        fset["fields"] = [_create_field(self.obj, col) for col in cols_norm]

        typemap = {
            "esriGeometryPoint": _create_point_feature,
            "esriGeometryPolyline": _create_polyline_feature, # TODO
            "esriGeometryPolygon": _create_polygon_feature, # TODO
            "esriGeometryMultipoint": _create_multipoint_feature, # TODO
        }

        fset["features"] = [ typemap[self.geom_type](r, sr=self.spatial_reference) 
                                for r in self.obj.to_dict('records') ]
        return fset

    def sr(self, sr=None):
        """
        Set the spatial reference for the dataset - defaults to 4326
        returns DataFrame object
        """
        default_sr = {"wkid": 4326}
        if sr is None:
            self.spatial_reference = SpatialReference(default_sr)
        elif isinstance(sr, SpatialReference):
            self.spatial_reference = SpatialReference(sr)
        elif isinstance(sr, int):
            default_sr["wkid"] = sr
            self.spatial_reference = SpatialReference(default_sr)
        elif isinstance(sr, dict):
            if "spatialReference" in sr.keys():
                if (
                    "wkid" in sr["spatialReference"].keys()
                    or "latestWkid" in sr["spatialReference"].keys()
                ):
                    self.spatial_reference = SpatialReference(sr)
                else:
                    raise ValueError(
                        "Spatial reference must specify 'wkid' or 'latestWkid'"
                    )
        elif isinstance(sr, str):
            try:
                sr = int(sr)
                default_sr["wkid"] = sr
                self.spatial_reference = SpatialReference(default_sr)
            except:
                raise ValueError(
                    "Cannot interpret spatial reference: pass an EPSG code, SpatialReference ",
                    " object, or dict",
                )
        else:
            raise ValueError(
                "Cannot interpret spatial reference: pass an EPSG code, SpatialReference object,",
                " or dict with 'wkid' property",
            )
        return self

    def geometry(self, geom_type):
        """
        Given an arcgis.geometry geometry object [Point, Multipoint, Polygon, Polyline],
        sets the spatially enabled kdf's geometry type as a string representation and
        returns the accessed dataframe
        """
        self.geom_type = _get_geometry_type(geom_type)
        return self

    def from_layer(self, layer):
        """
        Convert feature layer to spatially-enabled Koalas DF
        """
        raise NotImplementedError

    def to_dict(self):
        return self.__feature_set__()

    def to_featureset(self):
        return FeatureSet.from_dict(self.__feature_set__())


def _get_geometry_type(t):
    valid_types = {
        Point: "esriGeometryPoint",
        Polyline: "esriGeometryPolyline",
        MultiPoint: "esriGeometryMultipoint",
        Polygon: "esriGeometryPolygon",
    }

    try:
        return valid_types[t]
    except KeyError:
        raise TypeError(
            f"Geometry type must be one of: Point, Polyline, Multipoint, Polygon, not {t}"
        )


def _create_field(df, col):
    """
    Creates an Esri field for a given column in a dataframe
    TODO: see if adding support for pyspark.sql.types is needed
    TODO: add support for domain key
    """
    field = {"name": col, "alias": col}

    try:
        idx = df[col].first_valid_index()
        val = df[col].loc[idx]
    except:
        val = ""

    if isinstance(val, (str, np.str)):
        l = df[col].str.len().max()
        if str(l) == "nan":
            l = 255

        if not isinstance(l, int):
            try:
                l = int(l)
            except:
                l = 255

        field["type"] = "esriFieldTypeString"
        field["length"] = l
        return field

    elif isinstance(val, (datetime.datetime, pd.Timestamp, np.datetime64)):
        field["type"] = "esriFieldTypeDate"
        return field

    elif isinstance(val, (np.int32, np.int16, np.int8)):
        field["type"] = "esriFieldTypeSmallInteger"
        return field

    elif isinstance(val, (int, np.int, np.int64)):
        field["type"] = "esriFieldTypeBigInteger"
        return field

    elif isinstance(val, (float, np.float64)):
        field["type"] = "esriFieldTypeDouble"
        return field

    elif isinstance(val, (np.float32)):
        field["type"] = "esriFieldTypeSingle"
        return field

    else:
        raise TypeError(f"Unsupported column type: {type(val)}")

# TODO: _create_feature implementations
def _create_point_feature(record, sr, x_col='x', y_col='y', geom_key=None, exclude = []):
    """
    Create an esri point feature object from a record
    """
    feature = {}
    if geom_key is not None:
        feature['SHAPE'] = { 'x': record[geom_key][x_col], 
                                'y': record[geom_key][y_col],
                                'spatialReference': sr }
        feature['attributes'] = { k:v for k, v in record.items() if k != geom_key and k != 'SHAPE'\
                                and k not in feature['SHAPE'].keys() and k not in exclude }
    else:
        feature['SHAPE'] = {'x': record[x_col], 'y': record[y_col], 'spatialReference': sr}
        feature['attributes'] = { k:v for k,v in record.items() if k != 'SHAPE' and \
                                k not in feature['SHAPE'].keys() and k not in exclude }
    return feature

def _create_multipoint_feature(record):
    """
    Create an esri multipoint feature object from a record
    """
    raise NotImplementedError

def _create_polyline_feature(record):
    raise NotImplementedError

def _create_polygon_feature(record):
    raise NotImplementedError

# alternative code for creating fields from koalas dataframes
# abandoned because a) it was meant to improve readability it
# didn't do that imo and b) it was meant to improve speed and
# it was not consistently faster than the original approach

# def _alt_create_field(df, col):
#     try:
#         idx = df[col].first_valid_index()
#         val = df[col].loc[idx]
#     except:
#         val = ""

#     field = {
#         "name": col,
#         "alias": col,
#         "type": _get_esri_type(val) # maps type(val) through dict
#     }

#     if field["type"] == "esriFieldTypeString":
#         l = df[col].str.len().max() # this is sorta buggy auto-assignment - why infer this won't get longer in the future?
#         if str(l) == "nan":
#             l = 255

#         if not isinstance(l, int):
#           try:
#               l = int(l)
#           except:
#               l = 255
#         field["length"] = l

#     return field

# supplemental function for _alt_create_fields, may be generally
# useful later on since this is needed sometimes, but I feel a
# little uneasy about the implementation, can't place why...
# def _get_esri_type(val):
#     """
#     Return the string representation of an esri field type
#     """
#     esri_str = "esriFieldTypeString"
#     esri_date = "esriFieldTypeDate"
#     esri_small_int = "esriFieldTypeSmallInteger"
#     esri_big_int = "esriFieldTypeBigInteger"
#     esri_double = "esriFieldTypeDouble"
#     esri_single = "esriFieldTypeSingle"

#     typemap = {
#         str: esri_str,
#         np.str: esri_str,
#         datetime.datetime: esri_date,
#         pd.Timestamp: esri_date,
#         np.datetime64: esri_date,
#         np.int32: esri_small_int,
#         np.int16: esri_small_int,
#         np.int8: esri_small_int,
#         int: esri_big_int,
#         np.int: esri_big_int,
#         np.int64: esri_big_int,
#         float: esri_double,
#         np.float64: esri_double,
#         np.float32: esri_single
#     }

#     try:
#         return typemap[type(val)]
#     except KeyError:
#       raise TypeError(f"Unsupported column type: {type(val)}")
