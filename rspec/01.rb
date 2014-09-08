# -*- coding: utf-8 -*-
require 'rubygems'
require 'rspec'

require 'hdfs_jruby'

HDFS_TMP_DIR="./test_rspec.$$"

describe "test1" do
  before(:all) do
    Hdfs.mkdir(HDFS_TMP_DIR)
  end
  
  it "put test_dir" do
    Hdfs.put("./rspec/test_data", HDFS_TMP_DIR)
  end
  
  it "ls #{HDFS_TMP_DIR}/test_data" do
    cnt = 0
    Hdfs.ls("#{HDFS_TMP_DIR}/test_data").each do | stat |
      #p stat
      cnt+=1
    end
    expect(cnt).to eq 3
  end
  it "ls #{HDFS_TMP_DIR}/test_data use block" do
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

  after(:all) do
    Hdfs.delete(HDFS_TMP_DIR, true)
  end

end

