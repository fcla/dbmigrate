#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'
require "daitss"

include Daitss
archive
include DbMigrate 
DbMigrate.setup(archive)

if ARGV.length == 1
  DbMigrate.migrate_rejects ARGV[0]
else
  DbMigrate.migrate_rejects
end
