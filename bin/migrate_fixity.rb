#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'
require "daitss"

include Daitss
archive
include DbMigrate 
DbMigrate.setup(archive)
DbMigrate.migrate_fixity
