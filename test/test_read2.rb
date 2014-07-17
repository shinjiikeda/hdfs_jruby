
require 'hdfs_jruby/file'

file = ARGV[0]
offset = ARGV[1].to_i

Hdfs::File.open(file, "r") do |io|
io.seek(offset)
print io.read
io.close
end
