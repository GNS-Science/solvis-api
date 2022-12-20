#!python

import json
import logging
from datetime import datetime as dt

import geopandas as gpd
import shapely
import solvis
from flask_restx import Namespace, Resource, fields, reqparse
from solvis_store.solvis_db_query import matched_rupture_sections_gdf

from solvis_api.cloudwatch import ServerlessMetricWriter

# Set up your local config, from environment variables, with some sone defaults
from solvis_api.config import API_KEY, API_URL, CLOUDWATCH_APP_NAME, S3_URL
from solvis_api.datastore.resources import location_by_id
from solvis_api.toshi_api.toshi_api import ToshiApi

db_metrics = ServerlessMetricWriter(lambda_name=CLOUDWATCH_APP_NAME, metric_name="MethodDuration")

log = logging.getLogger(__name__)

api = Namespace('solution_analysis', description='Solution analysis related operations')

headers = {"x-api-key": API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)


def locations_geojson(locations, radius_km):
    features = []
    for loc in locations:
        log.debug(f'LOC {loc}')
        item = location_by_id(loc)
        polygon = solvis.circle_polygon(radius_km * 1000, lat=item.get('latitude'), lon=item.get('longitude'))
        feature = dict(
            id=loc,
            type="Feature",
            geometry=shapely.geometry.mapping(polygon),
            properties={
                "title": item.get('name'),
                "stroke": "#555555",
                "stroke-opacity": 1.0,
                "stroke-width": 2,
                "fill": "#555555",
            },
        )
        features.append(feature)
    return features


solution_analysis_composite_geojson_model = api.model(
    'Solution Analysis Geojson',
    dict(
        solution_id=fields.String(required=True, description='The unique identifier for the Inversion Solution.'),
        location_ids=fields.String(
            required=True, description='The location identifiers, comma-delmited list e.g. `WLG,PMR,ZQN`'
        ),
        radius_km=fields.String(required=True, description='The rupture/location intersection radius in km.'),
        rupture_count=fields.Integer(),
        section_count=fields.Integer(),
        ruptures=fields.String(description='All the rupture sections as geojson'),
        locations=fields.String(description='The query location radius polygons as geojson'),
        error_message=fields.String(description='Any problems'),
    ),
)


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
        t0 = dt.utcnow()

        parser = reqparse.RequestParser()
        parser.add_argument(
            'min_rate', type=float, default=None, help='include ruptures with an Annual Rate above this value'
        )
        parser.add_argument(
            'max_rate', type=float, default=None, help='include ruptures with an Annual Rate above this value'
        )
        parser.add_argument(
            'min_mag', type=float, default=None, help='include ruptures with an Annual Rate above this value'
        )
        parser.add_argument(
            'max_mag', type=float, default=None, help='include ruptures with an Annual Rate above this value'
        )

        args = parser.parse_args() or {}
        log.debug(f'args {args}')

        location_ids = ','.join(sorted(location_ids.split(',')))

        try:
            rupture_sections_gdf = matched_rupture_sections_gdf(
                solution_id,
                location_ids,
                radius_km * 1000,
                min_rate=args.get('min_rate') or 1e-20,
                max_rate=args.get('max_rate'),
                min_mag=args.get('min_mag'),
                max_mag=args.get('max_mag'),
            )
        except Exception as err:
            log.error(err)
            api.abort(500)

        location_features = locations_geojson(location_ids.split(','), radius_km)

        rupture_count, section_count = 0, 0
        if rupture_sections_gdf is not None:
            rupture_count = rupture_sections_gdf['magnitude.count'].sum() if rupture_sections_gdf is not None else 0
            section_count = rupture_sections_gdf.shape[0]
            geojson = gpd.GeoDataFrame(rupture_sections_gdf).to_json()
        else:
            geojson = json.dumps(dict(type="FeatureCollection", features=[]))

        error_message = None
        if section_count > 10e3:
            error_message = "Too many rupture sections satisfy the query, please try a more selective query."
        elif section_count == 0:
            error_message = "No ruptures satisfy the query."

        result = dict(
            solution_id=solution_id,
            location_ids=location_ids,
            radius_km=radius_km,
            ruptures=geojson,
            locations=json.dumps(dict(type="FeatureCollection", features=location_features)),
            rupture_count=rupture_count,
            section_count=section_count,
            error_message=error_message,
        )
        log.debug(f"{solution_id}, {location_ids}, {radius_km}, {rupture_count} {section_count}")
        t1 = dt.utcnow()
        db_metrics.put_duration(__name__, f'{self.__class__.__name__}.get', t1 - t0)
        log.info(f'SolutionAnalysis for id:{solution_id}, locs:{location_ids}, radius:{radius_km} took {t1-t0}')
        return result
