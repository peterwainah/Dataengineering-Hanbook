
import sys
import os
base_dir=os.path.dirname(__file__) or '.'
parent_dir_path=os.path.abspath(os.path.join(base_dir,os.pardir))
sys.path.insert(0,parent_dir_path)

from Apache_Spark.Extraction_Transformation.etl import etl_spark

etl_spark()