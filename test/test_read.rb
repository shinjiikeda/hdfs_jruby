
require 'hdfs_jruby/file'

#f = Hdfs::File.new("test.txt", "r").to_io
#print f.read
#f.close


Hdfs::File.open("test.txt", "r").each do | line |
print line
end
