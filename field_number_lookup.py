class FieldNumberLookup(object):
    event_name = ''
    FN_airport = 0
    FN_lat = 0
    FN_lon = 0
    FN_delay = 0
    FN_timestamp = 0

    def __init__(self, e: str, fN_airport: int, fN_lat: int, fN_lon: int, fN_delay: int, fN_timestamp: int):
        self.event_name = e
        self.FN_airport = fN_airport - 1
        self.FN_lat = fN_lat - 1
        self.FN_lon = fN_lon - 1
        self.FN_delay = fN_delay - 1
        self.FN_timestamp = fN_timestamp - 1

    @staticmethod
    def create(event):
        if event == "arrived":
            return FieldNumberLookup(event, 13, 31, 32, 23, 22)
        else:
            return FieldNumberLookup(event, 9, 28, 29, 16, 15)
