
import re
import sys
import glob

for arg in sys.argv[2:]:
    for file in glob.iglob(arg):
    	for line in open(file, 'r'):
    		if re.search(sys.argv[1], line):
    			print line,

if __name__=="__main__":
    pass

#####  https://jee-appy.blogspot.com.es/2016/02/how-to-send-mail-on-ubuntu-or-linux.html
