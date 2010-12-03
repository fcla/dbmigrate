#!/usr/bin/env ruby
#require 'ruby-prof'

require 'rubygems'
require 'dbmigrate'
require "daitss"

include Daitss
archive
account = ARGV.shift or raise "ACCOUNT id required, usage migrate_all_by_account_project ACCOUNT PROJECT"
project = ARGV.shift or raise "PROJECT id required, usage migrate_all_by_account_project ACCOUNT PROJECT"

include DbMigrate 
#RubyProf.start
DbMigrate.setup(archive)
ieids = DbMigrate.get_d1_ieids(account, project)
    
DbMigrate.migrate_ieids(ieids, account, project)
# r = RubyProf.stop
# printer = RubyProf::GraphHtmlPrinter.new r
# open('profile.html', 'w') { |io| printer.print io, :min_percent=> 0 }