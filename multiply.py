import MapReduce
import sys

"""
Compute the matrix multiplication in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

def mapper(record):
  # key: matrix (a,b)  
  # value: i,j,value  
  key = record[0]  
  if key=='a':#since it's A*B  
       mr.emit_intermediate(key, [record[1],record[2],record[3]])#index a on i,j,value  
  else:  
       mr.emit_intermediate(key, [record[2],record[1],record[3]])#and b on j,i,value  

def reducer(key, list_of_values):
  #key: matrix a or b  
  #list_of_values: i,j,value for a and j,i,value for b  
  #mr.intermediate: contains all rows from both a and b in a dictionary keyed on a,b  
  a={}  
  b={}  
  if key=='a':#computing A*B  
       #I NEED THE MATRIX DIMENSIONS!!! (5)  
       #populate two dictionaries with our known values  
       for v in list_of_values:  
            a[(v[0], v[1])]=v[2]  
       for r in mr.intermediate['b']:  
            b[(r[0], r[1])]=r[2]  
       #and fill the blanks  
       for i in range(0,5):  
            for j in range(0,5):  
                 if (i,j) not in a.keys():  
                      a[(i,j)]=0  
                 if (j,i) not in b.keys():  
                      b[(j,i)]=0  
       result=0  
       #compute the multiplication A*Bij = SUM(Aik * Bkj) for k in 0..4  
       for i in range(0,5):  
            for j in range(0,5):  
                 for k in range(0,5):  
                      result+=a[(i,k)]*b[(j,k)]  
                 mr.emit((i,j,result))  
                 result=0

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
