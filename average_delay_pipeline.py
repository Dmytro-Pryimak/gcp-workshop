import argparse
import logging
from typing import Generator, Tuple, Dict

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Duration

from options.custom_options import CustomOptions


class ParseFlightRecordFn(beam.DoFn):
    def __init__(self, event_type):
        beam.DoFn.__init__(self)
        self.event_type = event_type
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_flight_record_errors')

    def process(self, element, *args, **kwargs) -> Generator[Dict, None, None]:
        try:
            if self.event_type == "arrived":
                indices = (13, 31, 32, 23, 22)
            else:
                indices = (9, 28, 29, 16, 15)
            keys = ('FN_airport', 'FN_lat', 'FN_lon', 'FN_delay', 'FN_timestamp')
            fields = element.decode('utf-8').split(',')
            res = {k: fields[indices[i] - 1] for i, k in enumerate(keys)}
            res['event_type'] = self.event_type
            yield res
        except Exception:
            self.num_parse_errors.inc()
            raise


class AirportDelaysFn(beam.DoFn):
    def process(self, element: Dict, *args, **kwargs) -> Generator[Tuple[Tuple, float], None, None]:
        yield (element['FN_airport'], element['FN_lat'], element['FN_lon']), float(element['FN_delay'])


class AirportTimestampsFn(beam.DoFn):
    def process(self, element: Dict, *args, **kwargs) -> Generator[Tuple[Tuple, str], None, None]:
        yield (element['FN_airport'], element['FN_lat'], element['FN_lon']), element['FN_timestamp']


class AirportNumFlightsFn(beam.DoFn):
    def process(self, element: Dict, *args, **kwargs) -> Generator[Tuple[Tuple, int], None, None]:
        yield (element['FN_airport'], element['FN_lat'], element['FN_lon']), 1


class JoinArrDepRecordsFn(beam.DoFn):
    """
    Join arrivals and departures PCollection values into airport stats dict ready to write into BQ
    """

    def process(self, element: Tuple[Tuple, Dict], *args, **kwargs) -> Generator[Dict, None, None]:
        airport, values = element

        arr_ts = values.get('arr_timestamp')
        if not arr_ts:
            arr_ts = [""]
        dep_ts = values.get('dep_timestamp')
        if not dep_ts:
            dep_ts = [""]

        arr_delay = values.get('arr_delay')
        if not arr_delay:
            arr_delay = [-999.0]

        dep_delay = values.get('dep_delay')
        if not dep_delay:
            dep_delay = [-999.0]

        arr_num_flights = values.get('arr_num_flights')
        if not arr_num_flights:
            arr_num_flights = [0]

        dep_num_flights = values.get('dep_num_flights')
        if not dep_num_flights:
            dep_num_flights = [0]

        airport_stats = {
            'airport': airport[0],
            'latitude': airport[1],
            'longitude': airport[2],
            'arr_delay': arr_delay[0] if arr_delay[0] > -998 else None,
            'dep_delay': dep_delay[0] if dep_delay[0] > -998 else None,
            'timestamp': arr_ts[0] if arr_ts[0] > dep_ts[0] else dep_ts[0],
            'num_flights': int(arr_num_flights[0]) + int(dep_num_flights[0])
        }

        yield airport_stats


class WindowStats:
    def __init__(self):
        self.delay = None
        self.timestamp = None
        self.num_flights = None


def run(options):
    with beam.Pipeline(options=options) as p:
        print(p.options.pipeline_project)
        output_table = f'{p.options.pipeline_project}:flights.streaming_delays'
        raw_schema = "airport:string,latitude:float,longitude:float," \
                     "timestamp:timestamp,dep_delay:float,arr_delay:float,num_flights:integer"

        arr = moving_avg_of(p.options, p, "arrived")
        dep = moving_avg_of(p.options, p, "departed")

        records_for_bq = (
                {'arr_delay': arr.delay, 'arr_timestamp': arr.timestamp, 'arr_num_flights': arr.num_flights,
                 'dep_delay': dep.delay, 'dep_timestamp': dep.timestamp, 'dep_num_flights': dep.num_flights}
                | "airport:cogroup" >> beam.CoGroupByKey()
                | "airport:stats" >> beam.ParDo(JoinArrDepRecordsFn())
        )

        records_for_bq | "airport:write_toBQ" >> beam.io.WriteToBigQuery(
            table=output_table,
            schema=raw_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


def moving_avg_of(options, pipeline, event):
    # Duration here
    avg_interval = Duration.of(int(60 * (float(options.averagingInterval) / float(options.speedupFactor))))
    frequency = Duration.of(int(avg_interval.micros / (2 * 10 ** 6)))
    topic = "projects/" + options.pipeline_project + "/topics/" + event

    flights = (pipeline
               | f"{event}:read from pubSub" >> beam.io.ReadFromPubSub(topic=topic)
               | f"{event}:window" >> beam.WindowInto(window.SlidingWindows(avg_interval, frequency))
               | f"{event}:parse" >> beam.ParDo(ParseFlightRecordFn(event))
               )

    stats = WindowStats()
    stats.delay = (flights
                   | f"{event}:airportdelay" >> beam.ParDo(AirportDelaysFn())
                   | f"{event}:avgdelay" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
                   )

    stats.timestamp = (
            flights
            | f"{event}:timestamps" >> beam.ParDo(AirportTimestampsFn())
            | f"{event}:lastTimeStamp" >> beam.CombinePerKey(max)
    )
    stats.num_flights = (
            flights
            | f"{event}:numflights" >> beam.ParDo(AirportNumFlightsFn())
            | f"{event}:total" >> beam.CombinePerKey(sum)
    )

    return stats


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description='Process pipeline')

    run(options=CustomOptions())
