from __future__ import absolute_import
import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystem import CompressionTypes
import datetime

class TranformationOnFile(beam.PTransform):
  def expand(self, pcoll):
    # Transform logic goes here.
    return (pcoll 
                | 'split records' >> beam.Map(lambda x: x.split(","))
                | 'filter transaction_amount greater than 20' >> beam.Filter(lambda x: float(x[3]) > 20)
                | 'Exclude all transactions made before the year 2010' >> beam.Filter(lambda x :datetime.datetime.strptime(x[0][:-4], '%Y-%m-%d %H:%M:%S').date().year > 2010)
                | 'Sum the total by date' >> beam.GroupBy(lambda x : str(datetime.datetime.strptime(x[0][:-4], '%Y-%m-%d %H:%M:%S').date())).aggregate_field(lambda x:float(x[3]), sum, 'total_amount')
                |'format json' >> beam.Map(lambda x: "{date:"+str(x[0])+",total_amount:"+str(x[1])+"}")
                )

def run(argv=None):

    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--input', dest='input', required=False,
                        help='table_name.',
                        default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')

    parser.add_argument('--output', dest='output', required=False,
                        help='Gcs location.',
                        default='gs://cloud-samples-data/output/results.jsonl.gz')

  
    known_args, pipeline_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     | 'Read Inpute File from GCS' >> beam.io.ReadFromText(known_args.input,skip_header_lines=True)
     | 'Transformation' >> TranformationOnFile()
     | 'write output' >> beam.io.WriteToText(known_args.output,compression_type=CompressionTypes.AUTO))


    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()