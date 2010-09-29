require 'dm-core'
require 'dm-types'

require 'DATA_FILE.rb'
require 'INT_ENTITY.rb'

module Daitss1
  def setup
  	DB_URL =  "mysql://root@localhost/daitss"
  	DataMapper.setup :default, DB_URL
  end
end