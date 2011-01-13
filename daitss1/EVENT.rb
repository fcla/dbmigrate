require 'dm-core'
require 'dm-types'

D1D2_Event_Map = {
  "I" => "ingest",
  "D" => "disseminate",
  "WO" => "withdraw",
  "WA" => "withdraw",
  "FC" => "fixitycheck",
  "N" => "normalize",
  "M" => "migrate",
}

D1D2Ops_Event_Map = {
  "I" => "ingest finished",
  "D" => "disseminate finished",
  "WO" => "withdraw finished",
  "WA" => "withdraw finished",
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

  # convert Daitss I event type to DAITSS II event type
  def toD2OpsEventType
    # convert Reingest event to dissemination event
	  if @EVENT_TYPE.eql?("I") && @EVENT_PROCEDURE.include?("SIP re-ingest")
	     return "disseminate finished"
  	else
	     return D1D2Ops_Event_Map[@EVENT_TYPE]
	  end
  end
  # if this is a withdrawn event
  def withdrawn?
    yes = false
    if @EVENT_TYPE.eql?("WO") || @EVENT_TYPE.eql?("WA")
      yes = true
    end
    return yes
  end
end
