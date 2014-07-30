# Hdfs Jruby

Jruby HDFS API

## Installation

Add this line to your application's Gemfile:

    gem 'hdfs_jruby'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install hdfs_jruby

## Usage

    require 'hdfs_jruby'
    
    ...
    
    Hdfs.ls(path) do | stat |
      p stat
    end


## Contributing

1. Fork it ( https://github.com/[my-github-username]/hdfs_jruby/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
