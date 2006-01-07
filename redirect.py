#!/usr/local/bin/python
#
# <module 'redirect'>
# 
# 	- Steven D. Majewski		<sdm7g@Virginia.EDU>
#
# Functions:
# 	tofile( file, func, *args )
# 	tostring( func, *args )   ==> string
# 	tolines( func, *args )	  ==> [ line0, line1, ... lineN ] 
# 
# Functions apply a function to args, either redirecting the output
# or returning it as a string or a readlines() like list of lines. 
#
# tofile will print (to the file) and return the value returned by 
# apply( func, *args ). The value is also in the last string in 
# tolines ( or the last line in tostring ). tolines and tostring,
# will print the value on the original sys.stdout as well (unless
# it's == None ).
# 
#  
# Class	StringFile()
#    	Methods:   [ all of the typical file object methods ] 
#		read(),write(),readline(),readlines(),writelines(),
#		seek(),tell(),flush(),close(),isatty()  [NO fileno()]
#
# Creates a file-like interface to a character array. 
# Write's append to the array; Read's return the characters in the array.
#
# Class Tee( file1, file2 [, ... filen ] ) 
# 	create a fileobject that writes it's output to all of the files.
# Class Echo( filein, fileout )
#	create a fileobject that automatically echo's whatever is read 
#	from filein to fileout. 
#
# An instance of a Tee object can be assigned to sys.stdout and sys.stderr, 
# and all Python output will be 'tee'-ed to that file. 
# Unfortunately, 'Echo'-ing stdin does not reproduce YOUR typed input to
# the interpreter, whose input comes via a different mechanism. 
# Implementing a 'dribble()' function, that logs all input and output to
# a file will require another trick. 
#
# 
# 
# 'tofile()' temporarily reassigns sys.stdout while doing func.
# 'tostring()' and 'tolines()' both call 'tofile()' with an instance
#  of StringFile().
# 
# 
# tofile( '|lpr', func, output )  
#  
import sys
import os


def filew( file ):
# file is a filename, a pipe-command, a fileno(), or a file object
# returns file.
	if not hasattr( file, 'write' ) :  
		if file[0] == '|' : file = os.popen( file[1:], 'w' )
		else: file = open( file, 'w' )
	return file 

def filer( file ):
# file is a filename, a pipe-command, or a file object
# returns file.
	if not hasattr( file, 'read' ) :  
		if file[-1] == '|' : file = os.popen( file[1:], 'r' )
		else: file = open( file, 'r' )
	return file 

def tofile( file, func, *args ):
# apply func( args ), temporarily redirecting stdout to file.
# file can be a file or any writable object, or a filename string.
# a "|cmd" string will pipe output to cmd. 
# Returns value of apply( func, *args )
	ret = None
	file = filew( file )
	sys.stdout, file = file, sys.stdout 
	try:
		ret = apply( func, args )
	finally:
		print ret
		sys.stdout, file = file, sys.stdout 	
	return ret

def tostring( func, *args ):
# apply func( *args ) with stdout redirected to return string.
	string = StringFile()
	apply( tofile, ( string, func ) + args ) 
	return string.read() 

def tolines( func, *args ):
# apply func( *args ), returning a list of redirected stdout lines.
	string = StringFile()
	apply( tofile, ( string, func ) + args ) 
	return string.readlines() 





from array import array

# A class that mimics a r/w file.
# strings written to the file are stored in a character array.
# a read reads back what has been written.
# Note that the buffer pointer for read is independent of write,
# which ALWAYS appends to the end of buffer. 
# Not exactly the same as file semantics, but it happens to be
# what we want! 
# Some methods are no-ops, or otherwise not bery useful, but
# are included anyway: close(), fileno(), flush(), 

class StringFile:
	def __init__( self ):
		self._buf = array( 'c' )
		self._bp = 0 
	def close(self):
		return self
#  On second thought, I think it better to leave this out 
#  to cause an exception, rather than letting someone try
#  posix.write( None, string )
#	def fileno(self):
#		return None 	
	def flush(self):
		pass
	def isatty(self):
		return 0
	def read(self, *howmuch ):
		buf = self._buf.tostring()[self._bp:]
		if howmuch: 
			howmuch = howmuch[0]
		else:
			howmuch = len( buf )
		ret = buf[:howmuch]
		self._bp = self._bp + len(ret)
		return ret
	def readline(self):
		line = ''
		for c in self._buf.tostring()[self._bp:] :
		     line = line + c 
		     self._bp = self._bp + 1 
		     if c == '\n' : return line
	def readlines(self):
		lines = []
		while 'True' :
			lines.append( self.readline() ) 
			if not lines[-1] : return lines[:-1]
	def seek(self, where, how ):
		if how == 0 :
			self._bp = where 
		elif how == 1 : 
			self._bp = self._bp + where 
		elif how == 2 : 
			self._bp = len(self._buf.tostring()) + where 
	def tell(self):
		return self._bp 
	def write(self, what ):
		self._buf.fromstring( what ) 
	def writelines( self, lines ):
		for eachl in lines: 
		   self._buf.fromstring( eachl )


class Tee:
# Tee( file1, file2 [, filen ] ) 
# creates a writable fileobject  where the output is tee-ed to all of
# the individual files. 
	def __init__( self, *optargs ):
		self._files = []
		for arg in optargs:
			self.addfile( arg )
	def addfile( self, file ):
		self._files.append( filew( file ) )
	def remfile( self, file ):
		file.flush()
		self._files.remove( file )
	def files( self ):
		return self._files
	def write( self, what ):
		for eachfile in self._files: 
			eachfile.write( what )
	def writelines( self, lines ): 
		for eachline in lines: self.write( eachline )
	def flush( self ):
		for eachfile in self._files:
			eachfile.flush()
	def close( self ):
		for eachfile in self._files:
			self.remfile( eachfile )  # Don't CLOSE the real files.
	def CLOSE( self ):
		for eachfile in self._files:
			self.remfile( eachfile ) 
			self.eachfile.close() 		
	def isatty( self ):
		return 0



class Echo:
	def __init__( self, input, *output ):
		self._infile = filer( input )
		if output : self._output = filew(output[0])
		else: self._output = None 
	def read( self, *howmuch ):
		stuff = apply( self._infile.read, howmuch )
		if output: self._output.write( stuff )
		return stuff
	def readline( self ):
		line = self._infile.readline()
		self._output.write( line )
		return line
	def readlines( self ):
		out = [] 
		while 1: 
			out.append( self.readline() )
			if not out[-1]: return out[:-1]
	def flush( self ):
		self._output.flush()
	def seek( self, where, how ):
		self._infile.seek( where, how )
	def tell( self ): return self._infile.tell()
	def isatty( self ) : return self._infile.isatty()
	def close( self ) :
		self._infile.close()
		self._output.close()



	

if __name__ == '__main__': 
    def testf( n ):
	for i in range( n ):
		print '._.'*10 + '[', '%03d' % i, ']' + 10*'._.'
    if hasattr( os, 'popen' ): 
	tofile( '|more', testf, 300 )
    print '\n# Last 10 lines "printed" by testf(): '
    print '# (the Python equivalent of \'tail\'.)' 
    for line in tolines( testf, 300 )[-10:] :
	print line[:-1]

