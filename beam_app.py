# Based on example https://beam.apache.org/documentation/sdks/python-streaming/ word_count_streaming

from __future__ import absolute_import

import argparse

import json
import datetime as dt

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

#%%
class Namespace:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        
class CountAndMeanFn(beam.CombineFn):
    def create_accumulator(self):
        return 0.0, 0
    
    def add_input(self, sum_count, input):
        (sum, count) = sum_count
        return sum + float(input['value']), count + 1
  
    def merge_accumulators(self, accumulators):
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)
  
    def extract_output(self, sum_count):
        (sum, count) = sum_count
        return {
          'processing_time': dt.datetime.utcnow()
            .strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
          'count': count,
          'mean': sum / count if count else float('NaN')
        }
        
# Filter functions
def within_plants(x, plants):
    return x['DevicedId'] in plants

def within_quality(x):
    return x['quality'] == 'true'

def within_signals(x, señales):
    return x['tag'].split('.')[-2] in señales

def mean_between(x, lims):
    return not(x['mean'] >= lims[0] and x['mean'] <= lims[1])

def main(plantas, señales, ventana, lims, argv=None, save_main_session=True):
    
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
	parser.add_argument(
	  '--output_topic',
	  required=True,
	  help=(
		  'Output PubSub topic of the form '
		  '"projects/<PROJECT>/topics/<TOPIC>".'))
	group = parser.add_mutually_exclusive_group(required=True)
	group.add_argument(
	  '--input_topic',
	  help=(
		  'Input PubSub topic of the form '
		  '"projects/<PROJECT>/topics/<TOPIC>".'))
	group.add_argument(
	  '--input_subscription',
	  help=(
		  'Input PubSub subscription of the form '
		  '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
	known_args, pipeline_args = parser.parse_known_args(argv)
      
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    
    pipeline = beam.Pipeline(options=pipeline_options)
    messages = (pipeline
                  | 'Read from pubsub' >>  beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
                  | 'To Json' >> beam.Map(lambda e: json.loads(e.decode('utf-8')))
                  | 'Filter Plants' >> beam.Filter(within_plants, plantas)
                  | 'Filter Signals' >> beam.Filter(within_signals, señales)
                  | 'Filter Quality' >> beam.Filter(within_quality)
                  | 'Window' >> beam.WindowInto(window.FixedWindows(ventana))
                  | 'Calculate Metrics' >> beam
                            .CombineGlobally(CountAndMeanFn())
                            .without_defaults()
                  | 'Posfilter' >> beam.Filter(mean_between, lims)
                  | 'Encode' >> beam.Map(lambda x: json.dumps(x).encode("utf-8"))
                )
    
    messages | beam.io.WriteToPubSub(known_args.output_topic)
    
    result = pipeline.run()
    result.wait_until_finish()
    
    return None


if __name__ == '__main__':
    plantas = ['Rioclaro']
    señales = ['322FN106M01', '321ML001N01F01']
    ventana = 30
    
    lims = [0.0, 140.0]
    
    main(plantas, señales, ventana, lims)