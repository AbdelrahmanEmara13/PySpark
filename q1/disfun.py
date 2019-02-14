
import sys
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
	if len(sys.argv) < 2:
	    print >> sys.stderr, "Usage: CountJPGs <file>"
	    exit(-1)
   


sc = SparkContext()
#init_data = [(1,"elem1"),(2,"elem1"),(2,"elem2"),(2,"elem2")]


init_data=sys.argv[1]
#init_data='/loudacre/init_data.txt'

output= sc.textFile(init_data).map(lambda line: line.split('\t')).map(lambda fields: (fields[0],list(fields[1:])))

output = output.groupByKey().map(lambda x : (x[0], list(x[1]))).collect()


for i in output:
	if i[0]!='':
		print i
		

#sc.stop()
#exit()

