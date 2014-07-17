
require 'hdfs_jruby/file'

#f = Hdfs::File.new("test.txt", "r").to_io
#print f.read
#f.close


file = ARGV[0]
offset = ARGV[1].to_i

Hdfs::File.open(file, "r") do | io |
  io.seek(offset)
  io.each do |line|
    print line
  end
end
