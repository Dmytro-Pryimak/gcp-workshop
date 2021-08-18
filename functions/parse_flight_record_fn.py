from typing import Generator, Tuple, Dict

import apache_beam as beam
from apache_beam.metrics import Metrics

from flight import Flight
from airport import AirportStats


class ParseFlightRecordFn(beam.DoFn):
    def __init__(self, event_type):
        super(ParseFlightRecordFn, self).__init__()
        self.event_type = event_type
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_flight_record_errors')

    def process(self, element, *args, **kwargs) -> Generator[Flight, None, None]:
        try:
            f = Flight(element.split(','), self.event_type)
            yield f
        except Exception:
            self.num_parse_errors.inc()


class AirportDelaysFn(beam.DoFn):
    def process(self, element: Flight, *args, **kwargs) -> Generator[Tuple[Tuple, float], None, None]:
        yield element.airport._as_tuple(), float(element.delay)


class AirportTimestampsFn(beam.DoFn):
    def process(self, element: Flight, *args, **kwargs) -> Generator[Tuple[Tuple, str], None, None]:
        yield element.airport._as_tuple(), element.timestamp


class AirportNumFlightsFn(beam.DoFn):
    def process(self, element: Flight, *args, **kwargs) -> Generator[Tuple[Tuple, int], None, None]:
        yield element.airport._as_tuple(), 1


class JoinArrDepRecordsFn(beam.DoFn):
    """
    Join arrivals and departures PCollection values into AirportStats object
    """
    def process(self, element, *args, **kwargs) -> Generator[AirportStats, None, None]:
        airport, values = element

        arr_ts = values.get('arr_timestamp', [""])[0]
        dep_ts = values.get('dep_timestamp', [""])[0]

        airport_stats = AirportStats(
            airport,
            values.get('arr_delay', [-999.0])[0],
            values.get('dep_delay', [-999.0])[0],
            arr_ts if arr_ts > dep_ts else dep_ts,
            int(values.get('arr_num_flights', [0])[0]
            ) + int(values.get('dep_num_flights', [0])[0])
        )

        yield airport_stats


class ToBQRowFn(beam.DoFn):
    """
    Prepare AirportStats object to write into BQ
    """
    def process(self, element: AirportStats, *args, **kwargs) -> Generator[Dict, None, None]:
        data = {
            'airport': element.airport[0],
            'latitude': element.airport[1],
            'longitude': element.airport[2],
            'timestamp': element.timestamp,
            'dep_delay': element.dep_delay if element.dep_delay > -998 else None,
            'arr_delay': element.arr_delay if element.arr_delay > -998 else None,
            'num_flights': element.num_flights
        }

        yield data
