
require 'hdfs_jruby/file'

#f = Hdfs::File.new("test.txt", "r").to_io
#print f.read
#f.close

f = Hdfs::File.open("test_w.txt", "w")
p f.print  "test..\ntest\n"
p f.print "test..\ntest\n"

f.close()
