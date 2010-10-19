require 'dm-core'
require 'dm-types'

class PROJECT

  include DataMapper::Resource

  storage_names[:daitss1] = 'PROJECT'

  property :CODE, String, :length => 32, :key => true
  property :DESCRIPTION, String, :length => 255
end