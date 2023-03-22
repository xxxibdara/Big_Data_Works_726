from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def parse_line(input):
    line = input.split(':')
    if len(line) > 1:
        path = line[1].split(' ')
        return (line[0], path) #(k, v): (node, edges)


def neighbours(input):
    key = input[0]
    values = input[1]
    for w in values[1]:
        yield (w, (key, values[0][1]+1))



def main(inputs, output, start, end):

    graph_edges = sc.textFile(inputs+'/links-simple-sorted.txt').map(parse_line)
    known_paths = sc.parallelize([(start,('-',0))]).cache() # initial the start point 

    for i in range(6):
        new_path = known_paths.join(graph_edges).flatMap(neighbours) #first produce the regular format paths
        known_paths = known_paths.union(new_path).reduceByKey(min) # choose the shortest path
        known_paths.saveAsTextFile(output + '/iter-' + str(i))
        if len(known_paths.lookup(end)) > 0:
            break
   
    end_point = known_paths.lookup(end) # the distance of the end point(destination)
    paths = []
    if len(end_point) > 0:
        cur = end
        paths.append(cur)   
        while cur != start:
            next = known_paths.lookup(cur)[0][0] # get the source of the prev point(destination)    
            paths.append(next)  
            cur = next
        paths.reverse()
        final_path = sc.parallelize(paths)
        final_path.coalesce(1).saveAsTextFile(output+'/path')
    else:
        print("No path exists")   

    


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    start = sys.argv[3]
    end = sys.argv[4]

    conf = SparkConf().setAppName('shortest path')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    main(inputs, output, start, end)