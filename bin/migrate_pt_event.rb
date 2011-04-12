#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'
require "daitss"

include Daitss
archive
include DbMigrate 
DbMigrate.setup(archive)

if ARGV.empty?
  DbMigrate.migrate_pt_event
elsif ARGV.length == 2
  DbMigrate.migrate_pt_event ARGV[0], ARGV[1]
end
