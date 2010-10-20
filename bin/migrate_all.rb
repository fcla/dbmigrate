#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'

dbm = DbMigrate.new
dbm.setup
dbm.migrate_all
