import apache_beam.testing.test_pipeline 
import unittest
from apache_beam.testing.util import assert_that, equal_to, is_empty, is_not_empty
from apache_beam.testing.test_pipeline import TestPipeline
import datetime

class DataflowTest(unittest.TestCase):
    data_enhancement = DataEnhancement()

    testdata=["2011-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
              "2011-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
              "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
              "2012-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
              "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95"]
    with TestPipeline() as p:
        pcoll = (
            p 
            | apache_beam.Create( testdata ) 
            | 'split records' >> apache_beam.Map(lambda x: x.split(","))
            | 'filter transaction_amount greater than 20' >> apache_beam.Filter(lambda x: float(x[3]) > 20)
            | 'Exclude all transactions made before the year 2010' >> apache_beam.Filter(lambda x :datetime.datetime.strptime(x[0][:-4], '%Y-%m-%d %H:%M:%S').date().year > 2010)
            | 'Sum the total by date' >> apache_beam.GroupBy(lambda x : str(datetime.datetime.strptime(x[0][:-4], '%Y-%m-%d %H:%M:%S').date())).aggregate_field(lambda x:float(x[3]), sum, 'total_amount')
            |'format json' >> apache_beam.Map(lambda x: "{date:"+str(x[0])+",total_amount:"+str(x[1])+"}")
        )
        assert_that(pcoll, equal_to(['{date:2011-01-09,total_amount:2042203.98}','{date:2012-01-09,total_amount:1021101.99}']),label='valid')
        
        
    ##Testing - python3 vm_task_test.py