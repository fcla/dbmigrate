require 'dm-core'
require 'dm-types'

require 'daitss1/DATA_FILE'
require 'daitss1/INT_ENTITY'
require 'daitss1/EVENT'
require 'daitss1/ACCOUNT_PROJECT'
require 'daitss1/ADMIN'
  	
DB_URL =  "mysql://root@localhost/daitss"

module Daitss1

  def Daitss1.read_all ieid
	@d1_entity = INT_ENTITY.get(ieid)   
	@d1_datafiles = DATA_FILE.all(:IEID => @entity)
	@d1_events = EVENT.all(:OID => @entity)
  end

  def Daitss1.read_all(account_id, project_id)
	act_prj = ACCOUNT_PROJECT.all(:ACCOUNT => account_id, :PROJECT => project_id)
	ieids = ADMIN.all(:ACCOUNT_PROJECT => act_prj)
  end

end