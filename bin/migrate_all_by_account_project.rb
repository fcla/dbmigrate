#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'

account = ARGV.shift or raise "ACCOUNT id required, usage migrate_all_by_account_project ACCOUNT PROJECT"
project = ARGV.shift or raise "PROJECT id required, usage migrate_all_by_account_project ACCOUNT PROJECT"

include DbMigrate 
DbMigrate.setup
ieids = DbMigrate.get_d1_ieids(account, project)

DbMigrate.migrate_ieids(ieids, account, project)
