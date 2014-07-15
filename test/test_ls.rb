
require 'hdfs_jruby'



list = Hdfs.ls(ARGV[0])

list.each do | f |
  path = f.getPath.to_s
  size = f.getLen.to_i
  is_dir = f.isDir
  owner = f.getOwner
  group = f.getGroup
  mtime = f.getModificationTime()

  print "#{is_dir ? "d" : "f"} #{path} #{size} #{owner}:#{group} #{mtime}\n"
end


