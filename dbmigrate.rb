require 'daitss1/daitss1'
require 'daitss/db'
require 'd1agents'

class DbMigrate
  include Daitss

  def setup
	DataMapper.setup(:default, "mysql://root@localhost/daitss")
	d2_adapter = DataMapper.setup(:daitss2, "postgres://daitss:topdrawer@localhost/daitss2")
	d2_adapter.resource_naming_convention = DataMapper::NamingConventions::Resource::UnderscoredAndPluralizedWithoutModule
    @d1agent = D1Agents.new
  end

  def create_agents
    @d1agent.agents.each do |a|
      DataMapper.repository(:daitss2) do 
        agent = PremisAgent.new
        note = String.new("In production from " + a.start_time + " to " + a.end_time)
	    agent.attributes = { :id => a.id, :name => 'daitss I', :type => 'software', :note => note}
	    agent.save
	  end
	end
  end

  def migrate ieid
	DataMapper.repository(:default) do
		@d1_entity = INT_ENTITY.get(ieid)   
		@d1_datafiles = DATA_FILE.all(:IEID => @d1_entity.IEID, :ORIGIN => 'DEPOSITOR')
		@d1_events = EVENT.all(:OID => @d1_entity.IEID)
	end
	
	d2_datafiles = Array.new
	@d1_datafiles.each do |df|
		d2_df = DataMapper.repository(:daitss2) { Datafile.new }
		d2_df.attributes = { :id => df.DFID, :size => df.SIZE, :create_date => df.CREATE_DATE,
		:origin => df.ORIGIN, :original_path => df.PACKAGE_PATH + df.FILE_TITLE, 
		:creating_application => df.CREATOR_PROG }
		d2_datafiles << d2_df
	end
	
	@agent = DataMapper.repository(:daitss2) { PremisAgent.get('info:fcla/daitss/v1.5.0') }
    puts @agent.inspect
	unless @agent
		@agent = DataMapper.repository(:daitss2) { PremisAgent.new }
		@agent.attributes = { :id => 'info:fcla/daitss/v1.5.0', :name => 'daitss I', :type => 'software'}
	end
	
	d2_events = Array.new
	puts @agent.inspect
	@d1_events.each do |e|
		d2_e = DataMapper.repository(:daitss2) { IntentityEvent.new }
		d2_e.attributes = { :id => e.ID, :idType => 'URI', :e_type => e.toD2EventType,
		:datetime => e.DATE_TIME, :outcome => e.OUTCOME, :relatedObjectId => ieid }
		#@agent.premis_events << d2_e
		d2_e.premis_agent = @agent
		d2_events << d2_e
	end
	
	DataMapper.repository(:daitss2) { @d2_entity = Intentity.new }
	@d2_entity.attributes = { :id => @d1_entity.IEID, :original_name => @d1_entity.PACKAGE_NAME, 
	  :entity_id => @d1_entity.ENTITY_ID, :volume =>  @d1_entity.VOL, :issue => @d1_entity.ISSUE, 
	  :title => @d1_entity.TITLE }
	
	total_size = 0
    d2_datafiles.each do |d2_df| 
		@d2_entity.datafiles << d2_df 
		total_size += d2_df.size
	end

	DataMapper.repository(:daitss2) do
	  package = Package.new
	  package.transaction do
        account = Account.get("ACT")
	    project = account.projects.first :id => "PRJ"
	    project.packages << package

	    package.sip = Sip.new :name => @d1_entity.PACKAGE_NAME
   	    package.sip.number_of_datafiles = d2_datafiles.size
        package.sip.size_in_bytes = total_size

	    package.intentity = @d2_entity
	    raise "error saving package records #{package.inspect}" unless package.save
	    raise "cannot save aip" unless @d2_entity.save
        d2_events.each {|e| raise "error saving event records #{e.inspect}" unless e.save }
	  end
	end
  end
end