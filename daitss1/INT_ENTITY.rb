require 'dm-core'

class INT_ENTITY
  include DataMapper::Resource

  storage_names[:default] = 'INT_ENTITY'

  property :IEID, String, :key => true
 
  property :PACKAGE_NAME, String, :length => 32, :required => true
  property :EXT_REC, String, :length => 64
  property :EXT_REC_TYPE, String, :length => 64
  
  property :ENTITY_ID, String, :length => 100
  property :VOL, String, :length => 4
  property :ISSUE, String, :length => 3
  property :TITLE, Text

end
