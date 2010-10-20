#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'

dbm = DbMigrate.new
dbm.setup
dbm.create_agents
dbm.migrate_accounts

