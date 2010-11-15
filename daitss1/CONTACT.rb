require 'dm-core'
require 'dm-types'

class CONTACT

  include DataMapper::Resource

  storage_names[:daitss1] = 'CONTACT'

  property :ID, Integer, :key => true
  property :NAME, String, :length => 255
  property :ADDR_L1, String, :length => 128
  property :ADDR_L2, String, :length => 128
  property :ADDR_L3, String, :length => 128
  property :ADDR_L4, String, :length => 128
  property :ADDR_L5, String, :length => 128
  property :EMAIL, String, :length => 128
  property :PHONE, String, :length => 32
end
