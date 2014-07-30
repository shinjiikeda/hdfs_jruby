# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'hdfs_jruby/version'

Gem::Specification.new do |spec|
  spec.name          = "hdfs_jruby"
  spec.version       = Hdfs::VERSION
  spec.authors       = ["shinji ikeda"]
  spec.email         = ["gm.ikeda@gmail.com"]
  spec.summary       = %q{ jruby hdfs api}
  spec.description   = %q{}
  spec.homepage      = "https://github.com/shinjiikeda/hdfs_jruby"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
end
