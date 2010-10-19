require 'dm-core'
require 'dm-types'

D1D2_Event_Map = {
  "I" => "ingest",
  "D" => "disseminate",
  "WO" => "withdraw",
  "FC" => "fixitycheck",
  "N" => "normalize",
  "M" => "migrate",
}

class EVENT
  include DataMapper::Resource

  storage_names[:daitss1] = 'EVENT'

  property :ID, Integer, :key => true
  property :OID, String, :length => 16
 
  property :EVENT_TYPE, String
  property :DATE_TIME, DateTime
  property :EVENT_PROCEDURE, String, :length => 255
  property :OUTCOME, String
  property :NOTE, Text
  property :REL_OID, String, :length => 16

  # convert Daitss I event type to DAITSS II event type
  def toD2EventType
    # convert Reingest event to dissemination event
	if @EVENT_TYPE.eql?("I") && @EVENT_PROCEDURE.include?("SIP re-ingest")
	   return "disseminate"
	else
	   return D1D2_Event_Map[@EVENT_TYPE]
	end
  end
end
