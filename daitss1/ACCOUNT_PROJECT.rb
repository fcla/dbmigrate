require 'dm-core'
require 'dm-types'

class ACCOUNT_PROJECT

  include DataMapper::Resource

  storage_names[:default] = 'ACCOUNT_PROJECT'

  property :ID, Integer, :key => true
  property :ACCOUNT, String, :length => 16
  property :PROJECT, String, :length => 32
end