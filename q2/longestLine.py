
import sys
from pyspark import SparkContext


sc =SparkContext()

lines=sc.textFile('file:////home/training/training_materials/data/purplecow.txt')

lines_count=lines.map(lambda fields: (fields,len(fields)))

print 'longest line is [ '+ str(lines_count.max(key=lambda x:x[1])[0]) + '] with '+ str(lines_count.max(key=lambda x:x[1])[1])+ ' chars'


