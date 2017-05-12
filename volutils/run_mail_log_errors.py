"""
This is for running mail based alerts. Incomplete
"""
import re
import sys
import glob


if __name__=="__main__":
	for arg in sys.argv[2:]:
		for file in glob.iglob(arg):
			for line in open(file, 'r'):
				if re.search(sys.argv[1], line):
					print line,

# https://jee-appy.blogspot.com.es/2016/02/how-to-send-mail-on-ubuntu-or-linux.html
