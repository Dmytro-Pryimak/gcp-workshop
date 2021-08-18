from field_number_lookup import FieldNumberLookup


class Airport:
    name: str = None
    latitude: float = 0.0
    longitude: float = 0.0

    def __init__(self, fields: list, e: FieldNumberLookup):
        self.name = fields[e.FN_airport]
        self.latitude = fields[e.FN_lat]
        self.longitude = fields[e.FN_lon]

    def _as_tuple(self):
        return self.name, self.latitude, self.longitude


class AirportStats:
    """Airport stats object"""
    def __init__(self, airport: Airport, arr_delay: float, dep_delay: float, timestamp: str, num_flights: int):
        self.airport = airport
        self.arr_delay = arr_delay
        self.dep_delay = dep_delay
        self.timestamp = timestamp
        self.num_flights = num_flights
