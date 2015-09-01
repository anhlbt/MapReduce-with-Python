import MapReduce
import sys

"""
Unique trimmed nucleotide strings in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# Implement MapReduce phases
# =============================
def mapper(record):
    # key: sequence id
    # value: nucleotides
    trimmed = record[1][:-10]
    mr.emit_intermediate(trimmed, 0)

def reducer(key, list_of_values):
    # intermediate is dictionary has unique keys
    mr.emit(key)

# Run program
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
