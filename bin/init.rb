#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'

include DbMigrate 
DbMigrate.setup
DbMigrate.create_agents
DbMigrate.migrate_accounts

