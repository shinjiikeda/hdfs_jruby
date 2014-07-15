
require 'hdfs_jruby'


p Hdfs.exists?(ARGV[0])
p Hdfs.file?(ARGV[0])
p Hdfs.directory?(ARGV[0])
p Hdfs.size(ARGV[0])


