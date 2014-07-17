
require 'hdfs_jruby/file'

#f = Hdfs::File.new("test.txt", "r").to_io
#print f.read
#f.close


file = ARGV[0]

Hdfs::File.open(file, "r").each do | line |
  print line
end
