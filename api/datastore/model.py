
from pynamodb.attributes import UnicodeAttribute, JSONAttribute, UTCDateTimeAttribute, NumberAttribute, NumberSetAttribute
from pynamodb.models import Model

# from pynamodb.attributes import Attribute
# from pynamodb.constants import BINARY

from datetime import datetime

import logging

logging.basicConfig()
log = logging.getLogger("pynamodb")
log.setLevel(logging.DEBUG)
log.propagate = True

class SolutionLocationRadiusRuptureSet(Model):
    class Meta:
        read_capacity_units = 10
        write_capacity_units = 20
        table_name = "SolutionLocationRadiusRuptureSet"

    #solution_location_radius = UnicodeAttribute(hash_key=True)
    solution_id = UnicodeAttribute(hash_key=True)
    location_radius = UnicodeAttribute(range_key=True) #eg WLG-10000

    #capture the secondary range attributes - maybe a separate/sub/attribure model??

    radius = NumberAttribute()
    location = UnicodeAttribute()
    ruptures = NumberSetAttribute() #Rupture Index,
    rupture_count = NumberAttribute()

class SolutionRupture(Model):
    class Meta:
        read_capacity_units = 10
        write_capacity_units = 20
        table_name = "SolutionRupture"

    solution_id = UnicodeAttribute(hash_key=True)
    rupture_index = NumberAttribute(range_key=True)

    magnitude = NumberAttribute()     # Magnitude,
    avg_rake = NumberAttribute()      # Average Rake (degrees),
    area_m2 = NumberAttribute()       # Area (m^2),
    length_m = NumberAttribute()      # Length (m),
    annual_rate = NumberAttribute()   # Annual Rate
    fault_sections = NumberSetAttribute()

class SolutionFaultSection(Model):
    class Meta:
        read_capacity_units = 10
        write_capacity_units = 20
        table_name = "SolutionFaultSection"

    solution_id = UnicodeAttribute(hash_key=True)
    section_index = NumberAttribute(range_key=True)
    fault_name = UnicodeAttribute()
    dip_degree = NumberAttribute()
    rake = NumberAttribute()
    low_depth = NumberAttribute()
    up_depth = NumberAttribute()
    dip_dir = NumberAttribute()
    aseismic_slip_factor = NumberAttribute()
    coupling_coeff = NumberAttribute()
    slip_rate = NumberAttribute()
    parent_id = NumberAttribute()
    parent_name = UnicodeAttribute()
    slip_rate_std_dev = NumberAttribute()
    geometry = UnicodeAttribute()

table_classes = (
    SolutionLocationRadiusRuptureSet,
    SolutionRupture,
    SolutionFaultSection
)

def set_local_mode(host="http://localhost:8000"):
    for table in table_classes:
        table.Meta.host = host

def drop_all(*args, **kwargs):
    """
    drop all the tables etc
    """
    log.info("Drop allcalled")
    for table in table_classes:
        if table.exists():
            table.delete_table()
            log.info(f"deleted table: {table}")

def migrate(*args, **kwargs):
    """
    setup the tables etc
    """
    log.info("Migrate called")
    for table in table_classes:
        if not table.exists():
            table.create_table(wait=True)
            log.info(f"Migrate created table: {table}")