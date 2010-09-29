require 'dm-core'
require 'dm-types'

class EVENT
  include DataMapper::Resource

  storage_names[:default] = 'EVENT'

  property :ID, Integer, :key => true
  property :OID, String, :length => 16
 
  property :EVENT_TYPE, String
  property :DATE_TIME, DateTime
  property :EVENT_PROCEDURE, String, :length => 255
  property :OUTCOME, String
  property :NOTE, Text
  property :REL_OID, String, :length => 16
end
