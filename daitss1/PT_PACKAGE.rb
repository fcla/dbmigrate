require 'dm-core'
require 'dm-types'

class PT_PACKAGE

  include DataMapper::Resource

  storage_names[:package_tracker] = 'PT_PACKAGE'

  property :PT_UID, Integer, :key => true
  property :PACKAGE_NAME, String, :length => 32
  property :ACCOUNT, String, :length => 50
  property :PROJECT, String, :length => 50
  property :TITLE, String, :length => 2000
  property :STATUS, Integer, :default => 1
  property :COMMENTS, String, :length => 2000
end
