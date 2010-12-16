require 'daitss1/daitss1'
require 'daitss/db'
require 'd1agents'
require 'entity'

include Process

module DbMigrate
  include Daitss

  D1_DB_URL = 'd1-database-url'
  D1_OPS_DB_URL = 'd1-ops-database-url'

  attr_reader :d1_db_url, :d1_ops_db_url  
  
  def setup(archive)
    
    @d1_db_url = archive.yaml[D1_DB_URL]
    @d1_ops_db_url = archive.yaml[D1_OPS_DB_URL]
    @storage_url = archive.storage_url
    DataMapper.setup(:daitss1, @d1_db_url)
    DataMapper.setup(:package_tracker, @d1_ops_db_url)
   #DataMapper::Logger.new(STDOUT, 0)

  end

  # create versioned daitss I agents, based on fda system diary
  def create_agents
    d1agent = D1Agents.instance
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
	act_prj = DataMapper.repository(:daitss1) { ACCOUNT_PROJECT.first(:ACCOUNT => account, :PROJECT => project) } 
	oids = DataMapper.repository(:daitss1).adapter.select("select OID from ADMIN where ACCOUNT_PROJECT = #{act_prj.ID} and OID like 'E%'")
	oids.each{|oid| ieids << oid}  
  ieids
  end

  def migrate_ieids(ieids, account, project)
    ieids.each do |ieid| 
      begin 
        package = DataMapper.repository(:default) { Package.get(ieid) }
        if package.nil?
          puts "#{Time.now} migrating #{ieid}"
          # pid = fork do
            entity = Entity.new(ieid, account, project, @storage_url)
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

  # migrate D1 contacts to D2 agents, assumes all accounts were previously migrated
  def migrate_contacts
    d1_contacts = DataMapper.repository(:daitss1) { CONTACT.all }
    d1_contacts.each do |contact|
      oreq = DataMapper.repository(:daitss1) { OUTPUT_REQUEST.first(:CONTACT => contact.ID) }

      unless oreq
        puts "#{contact.NAME} not in OUTPUT_REQUEST table, skipping" 
        next
      end

      d1_account = oreq.ACCOUNT
      d1_id = oreq.ID
      can_disseminate = oreq.CAN_REQUEST_DISSEMINATION == "TRUE"
      can_withdraw = oreq.CAN_REQUEST_WITHDRAWAL == "TRUE"

      puts "migrating #{contact.NAME}"

      d2_account = DataMapper.repository(:default) { Account.get d1_account }
      d2_user = DataMapper.repository(:default) { Contact.first_or_create(:id => d2_account.id + d1_id.to_s) }

      unless d2_account
        puts "#{contact.NAME}'s account #{d1_account} not in D2 accounts table, skipping" 
        next
      end
       
      d2_user.first_name = contact.NAME.split(" ", 2)[0]
      d2_user.last_name = contact.NAME.split(" ", 2)[1]
      d2_user.email = contact.EMAIL
      d2_user.phone = contact.PHONE
      d2_user.address = ([contact.ADDR_L1, contact.ADDR_L2, contact.ADDR_L3, contact.ADDR_L4, contact.ADDR_L5].find_all { |l| l != "" }).join(';')
      d2_user.auth_key = rand(1000000)
      d2_user.description = "Contact migrated from D1"
      d2_user.account = d2_account
      d2_user.permissions = [:report, :submit, :peek]
      d2_user.permissions << :disseminate if can_disseminate
      d2_user.permissions << :withdraw if can_withdraw

      saved = DataMapper.repository(:default) { d2_user.save! }

      saved ? (puts contact.NAME + " migrated") : (puts contact.NAME + " not saved: ") # + d2_user.inspect + " d1: " + contact.inspect)
    end # of each
  end

  # creates package and sip records for uningested D1 packages in PT
  # creates an op event denoting the migration
  def migrate_uningested_from_pt
    adapter = DataMapper.repository(:package_tracker).adapter 
    res = adapter.select("SELECT * FROM PT_EVENT WHERE TARGET_PATH LIKE '%reject%' AND ACTION = 'INGESTF';")

    res.each do |reject|
      if DataMapper.repository(:package_tracker) { PT_EVENT.first(:PT_UID => reject["pt_uid"], :TARGET_PATH.like => "#E2%", :ACTION => "INGESTF") } # skip if package was subsequently ingested
        puts "skipping #{reject["pt_uid"]}, it was rejected but subsequently ingested"
        next
      end

      if DataMapper.repository(:default) { Event.first(:notes => "uid: #{reject["pt_uid"]}") }
        puts "skipping #{reject["pt_uid"]}, it was previously migrated"
        next
      end

      pt_package = DataMapper.repository(:package_tracker) { PT_PACKAGE.get reject["pt_uid"]}
      pt_register_event = DataMapper.repository(:package_tracker) { PT_EVENT.first(:PT_UID => reject["pt_uid"], :ACTION => "REGISTER") }
      puts "migrating uningested package #{pt_package.PACKAGE_NAME}"
      d2_account = DataMapper.repository(:default) { Account.get! pt_package.ACCOUNT }
      d2_project = d2_account.projects.first(:id => pt_package.PROJECT)

      unless d2_project
        puts "skipping #{pt_package.PACKAGE_NAME}, project #{pt_package.PROJECT} not in database"
        next
      end

      d2_package = DataMapper.repository(:default) { Package.new }
      d2_package.project = d2_project

      d2_sip = DataMapper.repository(:default) { Sip.new }
      d2_sip.name = pt_package.PACKAGE_NAME
      d2_sip.size_in_bytes = pt_register_event.SOURCE_SIZE
      d2_sip.number_of_datafiles = pt_register_event.SOURCE_COUNT
      
      d2_package.sip = d2_sip
      d2_package.log 'migrated from package tracker', :notes => "uid: #{pt_package.PT_UID}" 

      puts pt_package.PACKAGE_NAME + " migrated" if DataMapper.repository(:default) { d2_package.save }
    end
  end

  # migrates PT event records to D2 ops events table
  def migrate_pt_event
    uid_ieid = {}

    # first, iterate over rejected PT packages to get a UID, IEID pair
    adapter = DataMapper.repository(:package_tracker).adapter 
    rejected = adapter.select("SELECT * FROM PT_EVENT WHERE TARGET_PATH LIKE '%reject%' AND ACTION = 'INGESTF';")

    # look for the migration event to find the IEID
    rejected.each do |reject|
      d2_mig_event = DataMapper.repository(:default) { Event.first(:name => "migrated from package tracker", :notes => "uid: #{reject["pt_uid"]}") }

      unless d2_mig_event
        puts "skipping #{reject["pt_uid"]} as it doesn't appear to have been migrated into d2"
        next
      end

      uid_ieid[reject["pt_uid"]] = d2_mig_event.package.id
    end

    # second, iterate over ingested PT packages to get a UID, IEID pair
    ingested = adapter.select("SELECT * FROM PT_EVENT WHERE TARGET_PATH LIKE '#E2%' AND ACTION = 'INGESTF';")

    ingested.each do |ingest|

      ieid = ingest["target_path"].strip.gsub("#", "")

      unless DataMapper.repository(:default) { Package.get ieid }
        puts "skipping #{ingest["pt_uid"]} as it doesn't appear to have been migrated into d2"
        next
      end

      uid_ieid[ingest["pt_uid"]] = ieid
    end

    # iterate over every UID => IEID pair, creating an op event for each PT event with given UID

    uid_ieid.each do |uid, ieid| 
      package = DataMapper.repository(:default) { Package.get!(ieid) }
      events = DataMapper.repository(:package_tracker) { PT_EVENT.all(:PT_UID => uid) }
      events.each do |event|
        # skip event if already migrated
        if DataMapper.repository(:package_tracker) { package.events.first(:timestamp => event.TIMESTAMP, :name => "legacy operations data") } 
          puts "skipping #{event.ACTION} for #{ieid}, it appears to have already been migrated"
          next
        end

        puts "migrating #{event.ACTION} event for #{ieid}"

        notes = []
        notes << "AGENT: " + event.AGENT.strip
        notes << "ACTION: " + event.ACTION.strip
        notes << "SOURCE_PATH: " + event.SOURCE_PATH.strip
        notes << "TARGET_PATH: " + event.TARGET_PATH.strip
        notes << "SOURCE_COUNT: " + event.SOURCE_COUNT.to_s.strip
        notes << "TARGET_COUNT: " + event.TARGET_COUNT.to_s.strip
        notes << "SOURCE_SIZE: " + event.SOURCE_SIZE.to_s.strip
        notes << "TARGET_SIZE: " + event.TARGET_SIZE.to_s.strip
        notes << "NOTE: " + event.NOTE.strip
        note_str = notes.join("\n")

        DataMapper.repository(:default) { package.log "legacy operations data", { :timestamp => event.TIMESTAMP, :notes => note_str } }
      end
    end
  end

  # migrating D1 fixity events to D2 ops event, assumes package already migrated in d2
  def migrate_fixity
    adapter = DataMapper.repository(:package_tracker).adapter 
    d1_packages = adapter.select("SELECT * FROM packages WHERE id LIKE 'E2%';")

    d1_packages.each do |d1_package|
      # if any bad fixity events, get the whole history
      # otherwise, just get the latest good one
      ieid = d1_package['id']
      puts "migrating fixity events for #{ieid}"
      if DataMapper.repository(:daitss1) { EVENT.first(:OID => ieid, :EVENT_TYPE => "FC", :OUTCOME => "FAIL") }
        fixity_events = DataMapper.repository(:daitss1) { EVENT.all(:OID => ieid, :EVENT_TYPE => "FC") }

        fixity_events.each do |event|
          Package.get(ieid).log  "legacy fixity event", {:timestamp => event.DATE_TIME, :note => "outcome: #{event.OUTCOME}; note: #{event.NOTE}"}
        end
      else
        event = DataMapper.repository(:daitss1) { EVENT.first(:OID => ieid, :EVENT_TYPE => "FC", :order => [ :DATE_TIME.desc ]) }

        Package.get(ieid).log  "legacy fixity event", {:timestamp => event.DATE_TIME, :note => "outcome: #{event.OUTCOME}; note: #{event.NOTE}"}
      end
    end
  end

end # of module DbMigrate
