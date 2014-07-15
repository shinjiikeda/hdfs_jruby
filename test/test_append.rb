
require 'hdfs_jruby/file'

begin
f = Hdfs::File.open("test_a.txt", "a")
p f.print  "test..\ntest\n"

f.close()
rescue org.apache.hadoop.security.AccessControlException => e
  STDERR.print "permission denied\n"
end
