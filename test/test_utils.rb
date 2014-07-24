
require 'hdfs_jruby'


p Hdfs.exists?(ARGV[0])
p Hdfs.file?(ARGV[0])
p Hdfs.directory?(ARGV[0])
p Hdfs.size(ARGV[0])
p Hdfs.set_owner(ARGV[0], "test", "test")
p Hdfs.chmod(ARGV[0], 0444)


