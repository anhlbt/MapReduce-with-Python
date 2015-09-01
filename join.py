import MapReduce
import sys

"""
Relational Join in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[1]
    value = record
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: orderid
    # value: list of records
    order = list_of_values[0]
    length = len(list_of_values)
    for i in range(1, length):
      out_value = order + list_of_values[i]
      mr.emit(out_value)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
