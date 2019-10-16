
from pyspark import SparkConf, SparkContext
import sys
import re, string

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    for w in wordsep.split(line):
        yield (w.lower(),1)

def add(x,y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k,v=kv
    return '%s %i' % (k,v)

# add more functions as necessary

def main(inputs, output):
    text = sc.textFile(inputs).repartition(8)		#Reading file and repartitioning it
    words = text.flatMap(words_once)			#Mapping func
    wordsFinal= words.filter(lambda xy:len(xy[0])!=0)	#filtering out zero length characters
    wordcount = wordsFinal.reduceByKey(add)		#Reduce func
    outdata = wordcount.sortBy(get_key).map(output_format)	#Sorting in key value pair
    outdata.saveAsTextFile(output)			#saving as text file
    # main logic starts here

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)





