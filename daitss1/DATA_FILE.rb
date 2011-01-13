require 'dm-core'
require 'dm-types'

class DATA_FILE
	include DataMapper::Resource
	
	storage_names[:daitss1] = 'DATA_FILE'

	property :DFID, String, :length => 16, :key => true
	property :IEID, String, :length => 16
	property :CREATE_DATE, DateTime
	property :FILE_COPY_DATE, Time
	property :DIP_VERSION, String, :length => 16
    property :ORIGIN, String
	property :ORIG_URI, String, :length => 255
	property :PACKAGE_PATH, String, :length => 255
	property :FILE_TITLE,  String, :length => 255
	property :FILE_EXT,  String, :length => 8
	property :FORMAT,  String, :length => 255
	property :CREATOR_PROG,  String, :length => 255	
	property :SIZE, Integer, :min => 0, :max => 2^31-1
	property :BYTE_ORDER, String
	property :IS_ROOT, String
	property :IS_GLOBAL, String
	property :IS_OBSOLETE, String
	property :CAN_DELETE, String
	property :ROLE, String
	property :PRES_LEVEL, String					
	
end