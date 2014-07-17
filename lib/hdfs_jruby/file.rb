
require 'hdfs_jruby'

module Hdfs
  
  require "delegate"
  import java.lang.String

  class File < Delegator
    def initialize(path, mode = "r")
      @conf = Hdfs::Configuration.new()
      @fs = Hdfs::FileSystem.get(@conf)

      @mode = mode
      if mode == "w"
        @stream = @fs.create(Hdfs::Path.new(path), false)
      elsif mode == "r"
        @stream = @fs.open(Hdfs::Path.new(path))
        @buf = java.nio.ByteBuffer.allocate(65536)
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
    
    def self.open(path, mode = "r")
      return File.new(path, mode).to_io
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
    
    def seek(offset)
      @stream.seek(offset)
    end
    
    def close
      @stream.close
      @fs.close
    end

    def __getobj__
      @stream
    end

    def __setobj__(obj)
      @stream = obj
    end
  end
end
