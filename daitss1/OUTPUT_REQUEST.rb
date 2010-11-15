require 'dm-core'
require 'dm-types'

class OUTPUT_REQUEST

  include DataMapper::Resource

  storage_names[:daitss1] = 'OUTPUT_REQUEST'

  property :ID, Integer, :key => true
  property :ACCOUNT, String, :length => 16
  property :CONTACT, Integer
  property :CAN_REQUEST_DISSEMINATION, String
  property :CAN_REQUEST_REPORT, String
  property :CAN_REQUEST_WITHDRAWAL, String
end
