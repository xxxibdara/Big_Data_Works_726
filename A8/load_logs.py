from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import sys, os, re, gzip, uuid
from datetime import datetime


def parse_line(input):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    return line_re.split(input)


def main(input_dir, output_kys, table):

    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(output_kys)
    prepared = session.prepare("INSERT INTO " + table + "(host, datetime, path, bytes, id) VALUES (?, ?, ?, ?, ?)")
    batch = BatchStatement()
    count = 0

    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                data = parse_line(line)

                if len(data) > 4 and data is not None:
                    host = data[1]
                    date_time = datetime.strptime(data[2],"%d/%b/%Y:%H:%M:%S")
                    path = data[3]
                    bytes = int(data[4])
                    id = uuid.uuid4()
                    batch.add(prepared, (host, date_time, path, bytes, id))
                    count += 1
                if count == 300: 
                    #package the batch statements
                    session.execute(batch)
                    batch.clear()
                    count = 0
    

if __name__ == '__main__':
    input_dir = sys.argv[1]
    output_kys = sys.argv[2]
    table = sys.argv[3]
    main(input_dir, output_kys, table)
