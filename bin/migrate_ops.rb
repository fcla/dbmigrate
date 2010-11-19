#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'

include DbMigrate 
DbMigrate.setup
DbMigrate.migrate_ops
