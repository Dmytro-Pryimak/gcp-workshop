import argparse
import logging

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Duration
from apache_beam.options.pipeline_options import PipelineOptions

from field_number_lookup import FieldNumberLookup
from flight import WindowStats
from functions.parse_flight_record_fn import (
    ParseFlightRecordFn, AirportDelaysFn, AirportNumFlightsFn, AirportTimestampsFn,
    JoinArrDepRecordsFn, ToBQRowFn
)
from options.custom_options import CustomOptions
from utils import create_schema


def run(options):
    with beam.Pipeline(options=options) as p:
        print(p.options.pipeline_project)
        output_table = f'{p.options.pipeline_project}:flights.streaming_delays'
        raw_schema = "airport:string,latitude:float,longitude:float," \
                     "timestamp:timestamp,dep_delay:float,arr_delay:float,num_flights:integer"
        schema = create_schema(raw_schema)

        arr = moving_avg_of(p.options, p, "arrived")
        dep = moving_avg_of(p.options, p, "departed")

        records_for_bq = (
                {'arr_delay': arr.delay, 'arr_timestamp': arr.timestamp, 'arr_num_flights': arr.num_flights,
                 'dep_delay': dep.delay, 'dep_timestamp': dep.timestamp, 'dep_num_flights': dep.num_flights}
                | "airport:cogroup" >> beam.CoGroupByKey()
                | "airport:stats" >> beam.ParDo(JoinArrDepRecordsFn())
                | "airport:to_bq_row" >> beam.ParDo(ToBQRowFn())
        )

        # records_for_bq | "airport:write_to_file" >> beam.io.WriteToText('/Users/izadnip/outputData.txt')

        records_for_bq | "airport:write_toBQ" >> beam.io.WriteToBigQuery(
            table=output_table,
            # schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


def moving_avg_of(options, pipeline, event):
    # Duration here
    avg_interval = Duration.of(abs(1000 * 60 * (float(options.averagingInterval) / float(options.speedupFactor))))
    frequency = avg_interval.micros / 2
    topic = "projects/" + options.pipeline_project + "/topics/" + event
    event_type = FieldNumberLookup.create(event)

    flights = (pipeline
               | f"{event}:read from pubSub" >> beam.io.ReadFromPubSub(topic=topic)
               # | f"{event}:readData" >> beam.io.ReadFromText('/Users/izadnip/inputData.csv')
               | f"{event}:window" >> beam.WindowInto(window.SlidingWindows(avg_interval, frequency))
               | f"{event}:parse" >> beam.ParDo(ParseFlightRecordFn(event_type))
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

    # parser.add_argument('--averagingInterval', default='15.0')
    # parser.add_argument('--speedupFactor', default='1.0')
    # parser.add_argument('--project')
    # parser.add_argument('--region')
    # parser.add_argument('--temp_location')
    # parser.add_argument('--staging_location')

    # args, pipeline_args = parser.parse_known_args()
    #
    # pipeline_options = PipelineOptions(pipeline_args)

    run(options=CustomOptions())
