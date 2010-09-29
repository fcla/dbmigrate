require 'dm-core'
require 'dm-types'

class PROJECT

  include DataMapper::Resource

  storage_names[:default] = 'PROJECT'

  property :CODE, String, :length => 32, :key => true
  property :DESCRIPTION, String, :length => 255
end