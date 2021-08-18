from airport import Airport
from field_number_lookup import FieldNumberLookup


class Flight(object):
    airport: Airport = None
    delay: int = None
    timestamp: str = None

    def __init__(self, fields: list, e: FieldNumberLookup):
        self.airport = Airport(fields, e)
        # TODO assume that need to change logic below
        self.delay = fields[e.FN_delay]
        self.timestamp = fields[e.FN_timestamp]


class WindowStats:
    def __init__(self):
        self.delay = None
        self.timestamp = None
        self.num_flights = None
