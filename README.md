This a simple stand_alone Spark application whihc ingest data from a source path as defined by input_file parmeter to two destination directories
named "raw_tables.csv" and  "aggregated_table.csv", Validation also done in input JSON file and if there are invalid records program would exit with a none-zero code.

A sample spark-submit commnd could be as follow:
spark-submit --py-files src.zip main.py --mode "insert" --input_file "c://OldComputer//PycharmProjects//spark//src//conf//data.json"

Assuming that input json file is located at "c://OldComputer//PycharmProjects//spark//src//conf//data.json" on local PC, This was tested sucessfully on Spark 3.0.1 and Python 3.7