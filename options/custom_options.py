from apache_beam.options.pipeline_options import PipelineOptions


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--averagingInterval', default='15.0')
        parser.add_argument('--speedupFactor', default='1.0')
        parser.add_argument('--pipeline_project')
