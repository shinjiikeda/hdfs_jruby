
require 'hdfs_jruby'

list = Hdfs.ls(ARGV[0])

list.each do |stat|
  #p stat
  path = stat['path']
  length = stat['length']
  type = stat['type']
  owner = stat['owner']
  group = stat['group']
  mtime = stat['modificationTime']

  print "#{type == "DIRECTORY"  ? "d" : "f"} #{path} #{length} #{owner}:#{group} #{mtime}\n"
end


