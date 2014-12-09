
require 'hdfs_jruby'

module Hdfs
  
  require "delegate"
  import java.lang.String

  class File < Delegator
    
    # @param [String] path
    # @param [String] mode 'r' read,  'w' write, 'a': apeend
    def initialize(path, mode = "r")
      #@conf = Hdfs::Configuration.new()
      #@fs = Hdfs::FileSystem.get(@conf)
      @fs = Hdfs.get_fs()
      
      @mode = mode
      if mode == "w"
        @stream = @fs.create(Hdfs::Path.new(path), false)
      elsif mode == "r"
        @stream = @fs.open(Hdfs::Path.new(path))
      elsif mode == "a"
        p = Hdfs::Path.new(path)
        if !@fs.exists(p)
          @stream = @fs.create(Hdfs::Path.new(path), false)
        else
          if ! @fs.isFile(p)
            raise "path: #{path} is not file"
          end
          @stream = @fs.append(Hdfs::Path.new(path))
        end
      end
    end
    
    # @example
    #  Hdfs::File.open("hoge.txt", "r") do | io |
    #    ...
    #  end
    #  
    #  Hdfs::File.open("hoge.txt", "r").each do | line |
    #    puts line
    #  end
    #
    # @param [String] path
    # @param [String] mode 'r' read,  'w' write, 'a': apeend 
    def self.open(path, mode = "r")
      if block_given?
        io = File.new(path, mode).to_io
        begin
          yield(io)
        ensure
          begin
            io.close
          rescue
          end
        end
      else
        return File.new(path, mode).to_io
      end
    end
    
    def syswrite(str)
      n = @stream.write(str.to_java_bytes)
      return n.to_i
    end

    def sysread(length, outbuf = "")
      buf = Java::byte[length].new

      n = @stream.read(buf)
      if n < 0
        return nil
      end
      outbuf << java.lang.String.new(buf, 0, n).to_s
    end
    
    # @private
    def seek(offset, whence = IO::SEEK_SET)
      @stream.seek(offset)
      0
    end
    
    def close
      @stream.close
      @fs.close
    end
    
    # @private
    def __getobj__
      @stream
    end
    
    # @private
    def __setobj__(obj)
      @stream = obj
    end
  end
end
