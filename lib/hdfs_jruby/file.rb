
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
      if block_given?
        io = File.new(path, mode).to_io
        begin
          yield(io)
        ensure
          io.close
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
    
    def seek(offset, whence = IO::SEEK_SET)
      @stream.seek(offset)
      0
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
