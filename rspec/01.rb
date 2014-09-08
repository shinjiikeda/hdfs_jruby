# -*- coding: utf-8 -*-

require 'rubygems'
require 'rspec'

require 'hdfs_jruby'
require 'hdfs_jruby/file'

HDFS_TMP_DIR="./test_rspec.#{$$}"
puts "hdfs tmpdir: #{HDFS_TMP_DIR}"

describe "test1" do
  before(:all) do
    Hdfs.mkdir(HDFS_TMP_DIR)
  end
  
  it "put test_dir" do
    Hdfs.put("./rspec/test_data", HDFS_TMP_DIR)
  end
  
  it "ls #{HDFS_TMP_DIR}/test_data use block" do
    cnt = 0
    Hdfs.ls("#{HDFS_TMP_DIR}/test_data").each do | stat |
      #p stat
      cnt+=1
    end
    expect(cnt).to eq 3
  end
  it "ls #{HDFS_TMP_DIR}/test_data" do
    r = Hdfs.ls("#{HDFS_TMP_DIR}/test_data")
    expect(r.size).to eq 3
  end
  
  it "exists #{HDFS_TMP_DIR}/test_data/a/a" do
    r = Hdfs.exists?("#{HDFS_TMP_DIR}/test_data/a/a")
    expect(r).to eq true
  end
  it "directoy? #{HDFS_TMP_DIR}/test_data/a/a" do
    r = Hdfs.directory?("#{HDFS_TMP_DIR}/test_data/a/a")
    expect(r).to eq true
  end

  it "file? #{HDFS_TMP_DIR}/test_data/a/a/test.txt" do
    r = Hdfs.file?("#{HDFS_TMP_DIR}/test_data/a/a/test.txt")
    expect(r).to eq true
  end
  
  it "size #{HDFS_TMP_DIR}/test_data/a/a/test.txt" do
    size = Hdfs.size("#{HDFS_TMP_DIR}/test_data/a/a/test.txt")
    expect(size).to eq 4
  end
  
  it "create #{HDFS_TMP_DIR}/test_data/d/a/test.txt" do
    Hdfs::File.open("#{HDFS_TMP_DIR}/test_data/d/a/test.txt", "w") do |io|
      io.puts("d/a")
    end
  end
  
  it "read #{HDFS_TMP_DIR}/test_data/d/a/test.txt" do
    content = nil
    Hdfs::File.open("#{HDFS_TMP_DIR}/test_data/d/a/test.txt", "r") do |io|
      content = io.read
    end
    expect(content).to eq "d/a\n"
  end
  
  it "create #{HDFS_TMP_DIR}/test_data/append_test/test.txt" do
    Hdfs::File.open("#{HDFS_TMP_DIR}/test_data/append_test/test.txt", "a") do |io|
      io.puts("1")
    end
    Hdfs::File.open("#{HDFS_TMP_DIR}/test_data/append_test/test.txt", "a") do |io|
      io.puts("2")
    end
    content = nil
    Hdfs::File.open("#{HDFS_TMP_DIR}/test_data/append_test/test.txt", "r") do |io|
      content = io.read
    end
    expect(content).to eq "1\n2\n"
  end
  
  it "delete not empty directory" do
    begin
      Hdfs.delete("#{HDFS_TMP_DIR}/test_data")
      r = false
    rescue Java::OrgApacheHadoopIpc::RemoteException => e
      if e.to_s =~ /is non empty/
        r = true
      else
        r = false
      end
    rescue
      r = false
    end
    expect(r).to eq true
  end

  it "delete directory" do
    Hdfs.delete("#{HDFS_TMP_DIR}/test_data", true)
  end
  
  after(:all) do
    Hdfs.delete(HDFS_TMP_DIR, true)
  end

end

