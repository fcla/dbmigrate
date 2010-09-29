require 'dm-core'
require 'dm-types'

class ACCOUNT

  include DataMapper::Resource

  storage_names[:default] = 'ACCOUNT'

  property :CODE, String, :length => 16, :key => true
  property :NAME, String, :length => 255
  property :DESCRIPTION, String, :length => 255
  property :ADMIN_CONTACT, Integer
  property :TECH_CONTACT, Integer
  property :REPORT_EMAIL, String, :length => 255
end