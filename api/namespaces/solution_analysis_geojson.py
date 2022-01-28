#!python

import io
import json
from functools import lru_cache
import pandas as pd
import geopandas as gpd
import shapely

from flask import current_app
from flask_restx import reqparse
from flask_restx import Namespace, Resource, fields
from api.toshi_api.toshi_api import ToshiApi

# Set up your local config, from environment variables, with some sone defaults
from api.config import (WORK_PATH, USE_API, API_KEY, API_URL, S3_URL, SNS_TOPIC_ARN, IS_OFFLINE)
from api.solvis import multi_city_events
#from api.datastore.datastore import get_ds

from solvis import InversionSolution, new_sol, section_participation, circle_polygon
from api.datastore.solvis_db_query import matched_rupture_sections_gdf
from api.datastore.resources import location_by_id

import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

api = Namespace('solution_analysis', description='Solution analysis related operations')

headers={"x-api-key":API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

def locations_geojson(locations, radius_km):
    features = []
    for loc in locations:
        print('LOC', loc)
        item = location_by_id(loc)
        polygon = circle_polygon(radius_km*1000, lat=item.get('latitude'), lon=item.get('longitude'))
        feature = dict(id=loc, type="Feature", geometry=shapely.geometry.mapping(polygon),
                    properties = { "title": item.get('name'),
                        "stroke": "#555555",
                        "stroke-opacity": 1.0,
                        "stroke-width": 2,
                        "fill": "#555555"}
                    )
        features.append(feature)
    return features

solution_analysis_composite_geojson_model = api.model('Solution Analysis Geojson', dict(
    solution_id = fields.String(required=True, description='The unique identifier for the Inversion Solution.'),
    location_ids = fields.String(required=True, description='The location identifiers, comma-delmited list e.g. `WLG,PMR,ZQN`'),
    radius_km = fields.String(required=True, description='The rupture/location intersection radius in km.'),
    rupture_count = fields.Integer(),
    section_count = fields.Integer(),
    ruptures = fields.String(description='All the rupture sections as geojson'),
    locations = fields.String(description='The query location radius polygons as geojson'),
    error_message = fields.String(description='Any problems')
    ))

@api.route('/<string:solution_id>/loc/<string:location_ids>/rad/<int:radius_km>')
@api.param('solution_id', 'The inversion solution  identifier')
@api.param('location_ids', 'The location identifiers, comma-delmited e.g. `WLG,PMR,ZQN`')
@api.param('radius_km', 'The rupture/location intersection radius in km')
@api.response(404, 'solution analysis not found')
class SolutionAnalysisGeojsonComposite(Resource):
    """
    SolutionAnalysisGeojsonComposite handlers
    """
    @api.doc('get_solution_analysis_geojson ')
    @api.param('min_rate', 'restrict ruptures to those having a annual rate above the value supplied.')
    @api.param('max_rate', 'restricts ruptures to those having a annual rate below the value supplied.')
    @api.param('min_mag', 'restricts ruptures to those having a magnitude above the value supplied.')
    @api.param('max_mag', 'restricts ruptures to those having a magnitude below the value supplied.')
    @api.marshal_with(solution_analysis_composite_geojson_model)
    def get(self, solution_id, location_ids, radius_km):
        """
        GET handler
        """

        parser = reqparse.RequestParser()
        parser.add_argument('min_rate', type=float, default=None,
            help='include ruptures with an Annual Rate above this value')
        parser.add_argument('max_rate', type=float, default=None,
            help='include ruptures with an Annual Rate above this value')
        parser.add_argument('min_mag', type=float, default=None,
            help='include ruptures with an Annual Rate above this value')
        parser.add_argument('max_mag', type=float, default=None,
            help='include ruptures with an Annual Rate above this value')

        args = parser.parse_args() or {}
        print ('args', args, self)

        location_ids = ','.join(sorted(location_ids.split(',')))

        try:
            rupture_sections_gdf = matched_rupture_sections_gdf(solution_id, location_ids, radius_km*1000,
                min_rate=args.get('min_rate') or 1e-20,
                max_rate=args.get('max_rate'), min_mag=args.get('min_mag'), max_mag=args.get('max_mag'))
        except Exception as err:
            log.error(err)
            api.abort(500)

        location_features = locations_geojson(location_ids.split(','), radius_km)

        rupture_count, section_count = 0, 0
        if not rupture_sections_gdf is None:
            rupture_count = rupture_sections_gdf['magnitude.count'].sum() if not rupture_sections_gdf is None else 0
            section_count = rupture_sections_gdf.shape[0]
            geojson = gpd.GeoDataFrame(rupture_sections_gdf).to_json()
        else:
            geojson = json.dumps(dict(type="FeatureCollection", features= []))

        error_message = None
        if section_count > 10e3:
            error_message = "Too many rupture sections satisfy the query, please try a more selective query."
        elif section_count == 0:
            error_message = "No ruptures satisfy the query."

        result = dict(
            solution_id = solution_id,
            location_ids = location_ids,
            radius_km = radius_km,
            ruptures = geojson,
            locations = json.dumps(dict(type="FeatureCollection", features=location_features)),
            rupture_count = rupture_count,
            section_count = section_count,
            error_message = error_message
            )
        print(f"{solution_id}, {location_ids}, {radius_km}, {rupture_count} {section_count}")
        return result