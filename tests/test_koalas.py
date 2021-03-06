import contextlib
import datetime
import pytest
import random
import unittest
import warnings

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

from arcgis.geometry._types import (
    _is_valid,
    _is_point,
    _is_line,
    _is_polygon,
)

import databricks.koalas as ks
from databricks.koalas.extensions import (
    register_dataframe_accessor,
    register_series_accessor,
    register_index_accessor,
)

import numpy as np
import pandas as pd

import sparcgis

@pytest.fixture(scope='module')
def kdf():
    """
    Returns a dataframe with a few values for generic testing
    """
    return ks.DataFrame(
        {
            "x": [36.12, 47.32, 56.78, None],
            "y": [28.21, 87.12, 90.01, None],
            "names": ["geography", "place", "location", "geospatial"],
        }
    )

@pytest.fixture
def comprehensive_kdf():
    """
    Returns a koalas dataframe with all the types supported 
    Used for testing functionality where datatypes introduce cyclomatic complexity
    """
    from random import randrange as rr

    dates = [
        datetime.datetime(
            rr(1975, 2020), rr(1, 12), rr(1, 25), rr(00, 24), rr(00, 60), rr(00, 60)
        )
        for _ in range(5)
    ]

    kdf = ks.DataFrame(
        {
            "npint8": list(np.random.randint(-100, 100, 5, "int8")),
            "npint16": list(np.random.randint(-100, 100, 5, "int16")),
            "npint32": list(np.random.randint(-100, 100, 5, "int32")),
            "npint64": list(np.random.randint(-100, 100, 5, "int64")),
            "npint": list(np.random.randint(-100, 100, 5)),
            "int": [random.randint(-100, 100) for _ in range(5)],
            "float": [random.uniform(-100, 100) for _ in range(5)],
            "npfloat32": [np.float32(random.uniform(-100, 100)) for _ in range(5)],
            "npfloat64": [np.float64(random.uniform(-100, 100)) for _ in range(5)],
            "str": ["esri", "global", "geospatial", "intelligence", "cool"],
            "npstr": [
                np.str(x)
                for x in ["esri", "global", "geospatial", "intelligence", "cool"]
            ],
            "datetime": dates,
            "datetime64": [np.datetime64(d) for d in dates],
            "timestamp": [pd.Timestamp(d) for d in dates],
        }
    )

    return kdf


def test_it_works():
    assert sparcgis.__version__ == "0.1.0"


def test_geoaccessor(kdf):
    from sparcgis.koalas import KoalasGeoAccessor

    assert isinstance(kdf.spatial, KoalasGeoAccessor)


def test_sr(kdf):
    from sparcgis.koalas import KoalasGeoAccessor

    kdf.spatial.sr()
    assert kdf.spatial.spatial_reference is not None
    assert isinstance(kdf.spatial.spatial_reference, SpatialReference)
    assert kdf.spatial.spatial_reference["wkid"] == 4326


def test_fields(comprehensive_kdf):
    from sparcgis.koalas import KoalasGeoAccessor

    kdf = comprehensive_kdf

    def validate_field(field):
        # validates each field with 3 checks:
        return {
            # every field should have at least these keys: name, alias, type
            "key_check": list(field.keys()) >= ["name", "alias", "type"],
            # reverse lookup of a value in the df should return an instance of a supported type
            "type_check": isinstance(kdf[field["name"]].loc[0], supported_types),
            # every type field should match a valid string representation of an esri type
            "description_check": field["type"] in valid_esri,
        }

    valid_esri = [
        "esriFieldTypeString",
        "esriFieldTypeDate",
        "esriFieldTypeSmallInteger",
        "esriFieldTypeBigInteger",
        "esriFieldTypeDouble",
        "esriFieldTypeSingle",
    ]

    supported_types = (
        datetime.datetime,
        pd.Timestamp,
        np.datetime64,
        str,
        np.str,
        np.int32,
        int,
        np.int16,
        np.int8,
        float,
        np.float64,
        np.float32,
        np.int,
        np.int64,
    )

    fields = [sparcgis.koalas._create_field(kdf, col) for col in list(kdf.columns)]

    assert fields is not None
    assert all(list(map(lambda x: validate_field(x), fields)))


def test_to_dict(kdf):
    # TODO: refactor - this test covers most of the _create_point_feature logic
    # a separate set of tests should cover the feature creation
    # TODO: equivalency check for FeatureSet.to_dict and kdf.spatial.to_dict
    from sparcgis.koalas import KoalasGeoAccessor

    d = kdf.spatial.geometry(Point).to_dict()
    assert isinstance(d, dict)
    assert "fields" in d
    assert d["fields"] is not None
    assert "features" in d
    assert d["features"] is not None
    assert "SHAPE" in d["features"][0]
    pt = Point(d["features"][0]["SHAPE"])
    assert isinstance(pt, Point)
    assert pt.is_valid()


def test_to_featureset(kdf):
    # TODO: validity check for feature geometry
    from sparcgis.koalas import KoalasGeoAccessor

    kdf.spatial.sr().geometry(Point)
    fset = kdf.spatial.to_featureset()
    assert fset is not None
    assert isinstance(fset, FeatureSet)
    assert hasattr(fset, "features")
    assert hasattr(fset, "fields")
    assert hasattr(fset, "spatial_reference")
    assert fset.features is not None
    assert fset.fields is not None
    assert fset.geometry_type is not None


def test_to_featurelayer():
    pass


def test_to_feature_collection():
    pass
