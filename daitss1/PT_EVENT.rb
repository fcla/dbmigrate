require 'dm-core'
require 'dm-types'

class PT_EVENT

  include DataMapper::Resource

  storage_names[:daitss1] = 'PT_EVENT'

  property :ID, Integer, :key => true
  property :PT_UID, Integer
  property :AGENT, String, :length => 15
  property :ACTION, String
  property :SOURCE_PATH, String, :length => 255
  property :TARGET_PATH, String, :length => 255
  property :SOURCE_COUNT, Integer
  property :TARGET_COUNT, Integer
  property :SOURCE_SIZE, Integer, :min => 0, :max => 2**32
  property :TARGET_SIZE, Integer, :min => 0, :max => 2**32
  property :TIMESTAMP, DateTime
  property :NOTE, String, :length => 2000
end
