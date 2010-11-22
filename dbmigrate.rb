require 'daitss1/daitss1'
require 'daitss/db'
require 'd1agents'
require 'entity'

include Process

module DbMigrate
  include Daitss

  def setup
#	d1_adapter = DataMapper.setup(:daitss1, "mysql://daitss:topdrawer@localhost/daitss")
	d1_adapter = DataMapper.setup(:daitss1, "mysql://root@localhost/daitss")
	d2_adapter = DataMapper.setup(:default, "postgres://daitss2@localhost/daitss_db")
	d2_adapter.resource_naming_convention = DataMapper::NamingConventions::Resource::UnderscoredAndPluralizedWithoutModule
# 	DataMapper::Logger.new(STDOUT, 0)
  end

  # create versioned daitss I agents, based on fda system diary
  def create_agents
    d1agent = D1Agents.new
    d1agent.agents.each do |a|
      DataMapper.repository(:default) do 
        agent = PremisAgent.new
        note = String.new("In production from " + a.start_time.to_s + " to " + a.end_time.to_s)
	    agent.attributes = { :id => a.aid, :name => 'daitss I', :type => 'software', :note => note}
	    raise "cannot save agent #{agent.inspect}" unless agent.save
	  end
	end
  end

  # migrate all account, project records
  def migrate_accounts
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

  # migrate all daitss I packages to daitss II.
  def migrate_all
    d1_accounts = DataMapper.repository(:daitss1) { ACCOUNT.all }
    d1_accounts.each do |act|
      act_prjs = DataMapper.repository(:daitss1) { ACCOUNT_PROJECT.all(:ACCOUNT => act.CODE) }
       
      act_prjs.each do |act_prj|
        puts "migrating #{act.CODE},  #{act_prj.PROJECT}"
        ieids = get_d1_ieids(act.CODE, act_prj.PROJECT)
        migrate_ieids(ieids, act.CODE, act_prj.PROJECT)
      end
    end
  end

  # migrate all packages under account, project
  def get_d1_ieids(account, project)
  ieids = Array.new
	DataMapper.repository(:daitss1) do
		act_prj = ACCOUNT_PROJECT.first(:ACCOUNT => account, :PROJECT => project)   
		admins = ADMIN.all(:ACCOUNT_PROJECT => act_prj.ID, :OID.like => "E%")
	  admins.each { |admin| ieids << admin.OID }
	  admins = nil
  end

  ieids
  end

  def migrate_ieids(ieids, account, project)
    ieids.each do |ieid| 
      begin 
        package = DataMapper.repository(:default) { Package.get(ieid) }
        if package.nil?
          puts "#{Time.now} migrating #{ieid}"
          # pid = fork do
            entity = Entity.new(ieid, account, project)
            entity.migrate
            entity = nil
          # end
          # waitpid(pid, 0)
        end
      rescue => e
        puts "errors processing #{ieid}"
        puts e.message
        puts  e.backtrace.join("\n")
      end
    end
  end

end