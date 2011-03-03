require 'dm-core'
require 'dm-types'

class REJECTS

  include DataMapper::Resource

  storage_names[:rejects] = 'REJECTS'

  property :ID, Integer, :key => true
  property :PACKAGE_NAME, String, :length => 30
  property :REPORT_DATE, String, :length => 30
  property :RECIPIENT, String, :length => 80
  property :MESSAGE, String, :length => 300
  property :MAIL_FILE, String, :length => 64
  property :TIMESTAMP, Integer
  property :ACCOUNT, String, :length => 10
end
