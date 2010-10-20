#!/usr/bin/env ruby

require 'rubygems'
require 'dbmigrate'

account = ARGV.shift or raise "ACCOUNT id required, usage migrate_all_by_account_project ACCOUNT PROJECT"
project = ARGV.shift or raise "PROJECT id required, usage migrate_all_by_account_project ACCOUNT PROJECT"

dbm = DbMigrate.new
dbm.setup
dbm.migrate_account_project(account, project)