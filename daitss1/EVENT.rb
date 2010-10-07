require 'dm-core'
require 'dm-types'

D1D2_Event_Map = {
  "I" => "ingest",
  "D" => "disseminate",
  "WA" => "withdraw",
  "FC" => "fixitycheck",
  "N" => "normalize",
  "M" => "migrate",
}

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

  def toD2EventType
	return D1D2_Event_Map[@EVENT_TYPE]
  end
end
