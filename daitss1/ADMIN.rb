require 'dm-core'
require 'dm-types'

class ADMIN
  include DataMapper::Resource

  storage_names[:daitss1] = 'ADMIN'

  property :OID, String, :length => 16, :key => true
 
  property :INGEST_TIME, DateTime
  property :ACCOUNT_PROJECT, Integer
  property :SUB_ACCOUNT, Integer
end
