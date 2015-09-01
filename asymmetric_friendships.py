import MapReduce
import sys

"""
Social Network Analysis: Asymmetric friendships in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# Implement MapReduce phases
# =============================
def mapper(record):
    # key: person
    # value: friend
    person = record[0]
    friend = record[1]
    mr.emit_intermediate((person,friend), 1)
    mr.emit_intermediate((friend,person), 1)

def reducer(key, list_of_values):
    if len(list_of_values) < 2:
       mr.emit(key)

# Run program
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
