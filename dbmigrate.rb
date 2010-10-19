require 'daitss1/daitss1'
require 'daitss/db'
require 'd1agents'

class DbMigrate
  include Daitss

  def setup
	DataMapper.setup(:daitss1, "mysql://root@localhost/daitss")
	d2_adapter = DataMapper.setup(:default, "postgres://daitss:topdrawer@localhost/daitss2")
	d2_adapter.resource_naming_convention = DataMapper::NamingConventions::Resource::UnderscoredAndPluralizedWithoutModule
    @d1agent = D1Agents.new
  end

  # create versioned daitss I agents, based on fda system diary
  def create_agents
    @d1agent.agents.each do |a|
      DataMapper.repository(:default) do 
        agent = PremisAgent.new
        note = String.new("In production from " + a.start_time.to_s + " to " + a.end_time.to_s)
	    agent.attributes = { :id => a.id, :name => 'daitss I', :type => 'software', :note => note}
	    raise "cannot save agent #{agent.inspect}" unless agent.save
	  end
	end
  end

  # migrate all account, project records
  def migrate_all_accounts
    d1_accounts = DataMapper.repository(:daitss1) { ACCOUNT.all }
	d1_accounts.each do |act|
	  d2_act = DataMapper.repository(:default) {Account.new(:id => act.CODE, :description => act.NAME + " - " + act.DESCRIPTION) }
	  act_prjs = DataMapper.repository(:daitss1) { ACCOUNT_PROJECT.all(:ACCOUNT => act.CODE) }
	  
	  act_prjs.each do |act_prj|
	    puts act_prj.inspect
	    prj = DataMapper.repository(:daitss1) { PROJECT.first(:CODE => act_prj.PROJECT) }
	    puts prj.inspect
	    d2_prj = DataMapper.repository(:default) { Project.new }
	    d2_prj.attributes = { :id => prj.CODE, :description => prj.DESCRIPTION }
	    d2_act.projects << d2_prj
	  end
	
	  DataMapper.repository(:default) do
	  	d2_act.transaction do
	    	raise "cannot save account #{d2_act.inspect}" unless d2_act.save
	  	end
	  end
	end
	
  end

  # migrate all packages under account, project
  def migrate_all(account, project)
	act = DataMapper.repository(:default) { Account.get(account) }
	prj = DataMapper.repository(:default) { act.projects.first :id => project }
	
    ieids = Array.new
	DataMapper.repository(:daitss1) do
		act_prj = ACCOUNT_PROJECT.first(:ACCOUNT => account, :PROJECT => project)   
		admins = ADMIN.all(:ACCOUNT_PROJECT => act_prj.ID, :OID.like => "E%")
	    admins.each { |admin| ieids << admin.OID }
    end
    puts prj.inspect
    puts ieids.inspect
	ieids.each do |ieid| 
	  puts ieid
	  migrate(prj, ieid) 
	end
  end

  # migrate the package specified by ieid which will be inserted into d2 account, project
  def migrate_by_account_project(account, project, ieid)
    act = DataMapper.repository(:default) { Account.get(account) }
    prj = act.projects.first :id => project
    migrate prj, ieid
  end

  # migrate the package specified by ieid which will be inserted into d2 project (belonging to an account)
  def migrate(project, ieid)
	DataMapper.repository(:daitss1) do
		@d1_entity = INT_ENTITY.get(ieid)   
		@d1_datafiles = DATA_FILE.all(:IEID => @d1_entity.IEID, :ORIGIN => 'DEPOSITOR')
		# only migrate package level (ingest, dissemination and withdraw) events
		@d1_events = EVENT.all(:OID => @d1_entity.IEID, :EVENT_TYPE => 'I') + 
		  EVENT.all(:OID => @d1_entity.IEID, :EVENT_TYPE => 'WO')
	end
	
	d2_datafiles = Array.new
	@d1_datafiles.each do |df|
		d2_df = DataMapper.repository(:default) { Datafile.new }
		is_sip_descriptor = df.ROLE.eql?("DESCRIPTOR_SIP")
		d2_df.attributes = { :id => df.DFID, :size => df.SIZE, :create_date => df.CREATE_DATE,
		:origin => df.ORIGIN, :original_path => df.PACKAGE_PATH, :creating_application => df.CREATOR_PROG,
		:is_sip_descriptor => is_sip_descriptor }
		d2_datafiles << d2_df
	end
	
	d2_events = Array.new
	@d1_events.each do |e|
		d2_e = DataMapper.repository(:default) { IntentityEvent.new }
		d2_e.attributes = { :id => e.ID, :idType => 'URI', :e_type => e.toD2EventType,
		:datetime => e.DATE_TIME, :event_detail => e.NOTE, :outcome => e.OUTCOME, :relatedObjectId => ieid }
		
		# find the associated daitss I agent based on the event time
		d1_agent = @d1agent.find_agent(e.DATE_TIME)
		agent = DataMapper.repository(:default) { PremisAgent.get(d1_agent.id) }
		
		#@agent.premis_events << d2_e
		d2_e.premis_agent = agent
		d2_events << d2_e
	end
	
	d2_entity = DataMapper.repository(:default) { d2_entity = Intentity.new }
	d2_entity.attributes = { :id => @d1_entity.IEID, :original_name => @d1_entity.PACKAGE_NAME, 
	  :entity_id => @d1_entity.ENTITY_ID, :volume =>  @d1_entity.VOL, :issue => @d1_entity.ISSUE, 
	  :title => @d1_entity.TITLE }
	
	total_size = 0
    d2_datafiles.each do |d2_df| 
		d2_entity.datafiles << d2_df 
		total_size += d2_df.size
	end

	DataMapper.repository(:default) do
	  package = Package.new(:id => ieid)
	  package.transaction do
	    project.packages << package

	    package.sip = Sip.new :name => @d1_entity.PACKAGE_NAME
   	    package.sip.number_of_datafiles = d2_datafiles.size
        package.sip.size_in_bytes = total_size

	    package.intentity = d2_entity
	    raise "error saving package records #{package.inspect}" unless package.save
	    raise "cannot save aip" unless d2_entity.save
        d2_events.each {|e| raise "error saving event records #{e.inspect}" unless e.save }
	  end
	end
	
  end

end