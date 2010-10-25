require 'dm-core'

class COPY
  include DataMapper::Resource

  storage_names[:daitss1] = 'COPY'

  property :ID, Integer, :key => true
  property :IEID, String, :length => 16, :required => true
 
  property :SILO, Integer, :required => true
  property :PATH, String, :length => 255, :required => true
  property :MD5, String, :length => 32, :required => true

end