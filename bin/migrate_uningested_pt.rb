#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'
require "daitss"

include Daitss
archive
include DbMigrate 
DbMigrate.setup(archive)

if ARGV.empty?
  DbMigrate.migrate_uningested_from_pt
elsif ARGV.length == 2
  DbMigrate.migrate_uningested_from_pt ARGV[0], ARGV[1]
end
