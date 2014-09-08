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
      p stat
      cnt+=1
    end
    expect(cnt).to eq 3
  end
  
  after(:all) do
    Hdfs.delete(HDFS_TMP_DIR, true)
  end

end

