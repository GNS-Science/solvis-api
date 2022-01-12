
from pynamodb.attributes import UnicodeAttribute, JSONAttribute, UTCDateTimeAttribute, BinaryAttribute
from pynamodb.models import Model
from datetime import datetime

import logging

logging.basicConfig()
log = logging.getLogger("pynamodb")
log.setLevel(logging.DEBUG)
log.propagate = True

class SolutionLocationsRadiiDF(Model):
    class Meta:
        read_capacity_units = 10
        write_capacity_units = 20
        table_name = "solution_locations_radii_dataframes"

    id = UnicodeAttribute(hash_key=True) #use a UUID

    solution_id = UnicodeAttribute()
    locations_list_id = UnicodeAttribute()
    radii_list_id = UnicodeAttribute()
    created = UTCDateTimeAttribute(default=datetime.now)

    dataframe = BinaryAttribute()

def set_local_mode():
    SolutionLocationsRadiiDF.Meta.host = "http://localhost:8000"

def migrate(*args, **kwargs):
    """
    setup the tables etc
    """
    log.info("Migrate called")

    if not SolutionLocationsRadiiDF.exists():
        SolutionLocationsRadiiDF.create_table(wait=True)
        log.info("Migrate created table: SolutionLocationsRadiiDF")