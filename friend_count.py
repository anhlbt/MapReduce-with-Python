import MapReduce
import sys

"""
Social Network Analysis: Count Friends in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# Implement MapReduce phases
# =============================
def mapper(record):
    # key: username
    # value: friend's name
    key = record[0]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: username
    # value: list of friend counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Run program
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
