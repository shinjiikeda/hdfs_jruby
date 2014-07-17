
require "hdfs_jruby/version"

module Hdfs

  JAR_PATTERN_0_20="hadoop-core-*.jar"
  
  if RUBY_PLATFORM =~ /java/
    include Java
  else
    warn "only for use with JRuby"
  end
  
  if ENV["HADOOP_HOME"]
    HADOOP_HOME=ENV["HADOOP_HOME"]
    Dir["#{HADOOP_HOME}/#{JAR_PATTERN_0_20}","#{HADOOP_HOME}/lib/*.jar", "#{HADOOP_HOME}/share/hadoop/common/*.jar", "#{HADOOP_HOME}/share/hadoop/common/lib/*.jar", "#{HADOOP_HOME}/share/hadoop/hdfs/*.jar", "#{HADOOP_HOME}/share/hadoop/hdfs/lib/*.jar"].each  do |jar|
      require jar
    end
    $CLASSPATH << "#{HADOOP_HOME}/conf"
  else
    raise "HADOOP_HOME is not set!"
  end
  
  class FileSystem < org.apache.hadoop.fs.FileSystem
  end

  class Configuration < org.apache.hadoop.conf.Configuration
  end

  class Path < org.apache.hadoop.fs.Path
  end
  
  class FsPermission < org.apache.hadoop.fs.permission.FsPermission
  end
  
  @conf = Hdfs::Configuration.new()
  @fs = Hdfs::FileSystem.get(@conf)

  def list(path, use_glob=true)
    p = _path(path)
    if ! block_given?
      raise "error"
    else
      list = nil
      if use_glob
        list = @fs.globStatus(p)
      else
        list = @fs.listStatus(p)
      end
      list.each do | stat |
        file_info = {}
        file_info['path'] = stat.getPath.to_s
        file_info['length'] = stat.getLen.to_i
        file_info['modificationTime'] = stat.getModificationTime.to_i
        file_info['owner'] = stat.getOwner.to_s
        file_info['group'] = stat.getGroup.to_s
        file_info['permission'] = stat.getPermission.toShort.to_i
        file_info['type'] = !stat.isDir ? 'FILE': 'DIRECTORY'
        yield file_info
      end
    end
  end
  
  def exists?(path)
    @fs.exists(_path(path))
  end

  def move(src, dst)
    @fs.rename(Path.new(src), Path.new(dst))
  end

  def delete(path, r=false)
    @fs.delete(_path(path), r)
  end

  def file?(path)
    @fs.isFile(_path(path))
  end

  def directory?(path)
    @fs.isDirectory(_path(path))
  end

  def size(path)
    @fs.getFileStatus(_path(path)).getLen()
  end
  
  def mkdir(path)
    @fs.mkdirs(_path(path))
  end

  def put(local, remote)
    @fs.copyFromLocalFile(Path.new(local), Path.new(remote))
  end

  def get(remote, local)
    @fs.copyToLocalFile(Path.new(remote), Path.new(local))
  end
  
  def get_home_directory()
    @fs.getHomeDirectory()
  end
  
  def get_working_directory()
    @fs.getWorkingDirectory()
  end

  def set_working_directory(path)
    @fs.setWorkingDirectory(_path())
  end

  def set_permission(path, perm)
    @fs.setPermission(_path(path), org.apache.hadoop.fs.permission.FsPermission.new(perm))
  end
  
  def set_owner(path, owner, group)
    @fs.setOwner(_path(path), owner, group)
  end
  
  module_function :exists?
  module_function :move
  module_function :delete
  module_function :file?
  module_function :directory?
  module_function :size
  module_function :put
  module_function :get
  module_function :get_home_directory
  module_function :get_working_directory
  module_function :set_working_directory
  module_function :set_permission
  module_function :set_owner
  module_function :list

  private
  def _path(path)
    if path.nil?
      raise "path is nil"
    end
    Path.new(path)
  end

  module_function :_path
end
