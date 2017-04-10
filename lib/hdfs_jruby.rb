
require "hdfs_jruby/version"

module Hdfs

  JAR_PATTERN_0_20="hadoop-core-*.jar"
   
  if RUBY_PLATFORM =~ /java/
    include Java
  else
    warn "only for use with JRuby"
  end
  
  if ! ENV["HADOOP_HOME"] && File.exists?("/usr/lib/hadoop")
    ENV["HADOOP_HOME"] = "/usr/lib/hadoop"
  end
  
  if ENV["HADOOP_HOME"]
    HADOOP_HOME=ENV["HADOOP_HOME"]
    Dir["#{HADOOP_HOME}/#{JAR_PATTERN_0_20}",
        "#{HADOOP_HOME}/lib/*.jar",
        "#{HADOOP_HOME}/client/*.jar",
        "#{HADOOP_HOME}/share/hadoop/common/*.jar",
        "#{HADOOP_HOME}/share/hadoop/common/lib/*.jar",
        "#{HADOOP_HOME}/share/hadoop/hdfs/*.jar",
        "#{HADOOP_HOME}/share/hadoop/hdfs/lib/*.jar"
        ].each  do |jar|
      if File.symlink?(jar)
        link = File.readlink(jar)
        abs_path = File.expand_path(link, File.dirname(jar))
        next unless File.exist?(abs_path)
      end
      require jar
    end
    $CLASSPATH << "#{HADOOP_HOME}/conf"
    $CLASSPATH << "/etc/hadoop/conf"
  else
    raise "HADOOP_HOME is not set!"
  end
  
  # @private
  class FileSystem < org.apache.hadoop.fs.FileSystem
  end
  
  # @private
  class Configuration < org.apache.hadoop.conf.Configuration
  end
  
  # @private
  class Path < org.apache.hadoop.fs.Path
  end
  
  # @private
  class FsPermission < org.apache.hadoop.fs.permission.FsPermission
  end
  
  @conf = Hdfs::Configuration.new
  @fs = Hdfs::FileSystem.get(@conf)
  
  # @private
  def connectAsUser(user)
    uri =  Hdfs::FileSystem.getDefaultUri(@conf)
    @fs.close if ! @fs.nil?
    @fs = Hdfs::FileSystem.get(uri, @conf, user)
  end
  
  # ls
  # @example
  #   Hdfs.ls("hoge/").each do | stat |
  #     p stat
  #   end
  # @param [String] path 
  # @return [Array] file status array
  #
  # @note file status:
  #              path
  #              length
  #              modificationTime
  #              owner
  #              group
  #              permission
  #              type
  def ls(path)
    p = _path(path)
    list = @fs.globStatus(p)
    return [] if list.nil?

    ret_list = []
    list.each do |stat|
      if stat.isDir
        sub_list = @fs.listStatus(stat.getPath)
        next if sub_list.nil?
        
        sub_list.each do | s |
          if block_given?
            yield _conv(s)
          else
            ret_list << _conv(s)
          end
        end
      else
        if block_given?
          yield _conv(stat)
        else
          ret_list << _conv(stat)
        end
      end
    end
    ret_list if ! block_given?
  end
  
  # @private
  def list(path, opts={})
    use_glob = opts[:glob] ? true : false
    p = _path(path)

    list = nil
    if use_glob
      list = @fs.globStatus(p)
    else
      list = @fs.listStatus(p)
    end
    return [] if list.nil?
      
    if ! block_given?
      ret_list = []
      list.each do | stat |
        ret_list << _conv(stat)
      end
      return ret_list
    else
      list.each do | stat |
        yield _conv(stat)
      end
    end
  end
  
  # @param [String] path
  def exists?(path)
    @fs.exists(_path(path))
  end
  
  # @param [String] src hdfs source path
  # @param [String] dst hdfs destination path
  def move(src, dst)
    @fs.rename(Path.new(src), Path.new(dst))
  end
  
  # delete
  #
  # @param [String] path
  # @param [Boolean] r recursive false or true (default: false)
  def delete(path, r=false)
    @fs.delete(_path(path), r)
  end
  
  # @return [Boolean] true: file, false: directory
  def file?(path)
    @fs.isFile(_path(path))
  end

  # @return [Boolean] true: directory, false: file
  def directory?(path)
    @fs.isDirectory(_path(path))
  end
  
  # @return [Integer] file size
  def size(path)
    @fs.getFileStatus(_path(path)).getLen()
  end
  
  # make directory
  # @param [String] path
  def mkdir(path)
    @fs.mkdirs(_path(path))
  end
  
  # put file or directory to hdfs
  # @param [String] local surouce (local path)
  # @param [String] remote destination (hdfs path)
  def put(local, remote)
    @fs.copyFromLocalFile(Path.new(local), Path.new(remote))
  end

  # get file or directory from hdfs
  # @param [String] remote surouce (hdfs path)
  # @param [String] local destination (local path)
  def get(remote, local)
    @fs.copyToLocalFile(Path.new(remote), Path.new(local))
  end
  
  # get home directory
  def get_home_directory()
    @fs.getHomeDirectory()
  end
  
  # get working directory
  def get_working_directory()
    @fs.getWorkingDirectory()
  end
  
  # set working directory
  def set_working_directory(path)
    @fs.setWorkingDirectory(_path())
  end
  
  # set permission
  # @param [String] path
  # @param [Integer] perm permission
  def set_permission(path, perm)
    @fs.setPermission(_path(path), org.apache.hadoop.fs.permission.FsPermission.new(perm))
  end
  
  # set owner & group
  # @param [String] path
  # @param [String] owner
  # @param [String] group
  def set_owner(path, owner, group)
    @fs.setOwner(_path(path), owner, group)
  end
  
  def get_fs
    @fs
  end
   
  module_function :exists?
  module_function :move
  module_function :mkdir
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
  module_function :ls
  module_function :connectAsUser
  module_function :get_fs

  private
  
  # @private
  def _path(path)
    if path.nil?
      raise "path is nil"
    end
    Path.new(path)
  end
  
  # @private
  def _conv(stat)
    file_info = {}
    file_info['path'] = stat.getPath.to_s
    file_info['length'] = stat.getLen.to_i
    file_info['modificationTime'] = stat.getModificationTime.to_i
    file_info['owner'] = stat.getOwner.to_s
    file_info['group'] = stat.getGroup.to_s
    file_info['permission'] = stat.getPermission.toShort.to_i
    file_info['type'] = !stat.isDir ? 'FILE': 'DIRECTORY'
    return file_info
  end

  module_function :_path
  module_function :_conv

end
