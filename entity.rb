require 'daitss1/daitss1'
require 'daitss/db'
require 'd1agents'
require 'erb'

class Entity
  def initialize(ieid, account, project, storage_url)
    @ieid = ieid
    act = DataMapper.repository(:default) { Account.get(account) }
    @prj = DataMapper.repository(:default) { act.projects.first :id => project }
    @d1agent = D1Agents.instance
    @d1_stud_descriptor = XML::Document.file('daitss1.xml').to_s
    @storage_url = storage_url
    @d1_op_agent = Program.get("DAITSS1")
  end

  # migrate the package specified by ieid which will be inserted into d2 project (belonging to an account)
  def migrate
    withdrawn = false
    d2_events = Array.new

    # migrate int entity data from d1 to d2
    d1_entity = DataMapper.repository(:daitss1) {  INT_ENTITY.get(@ieid)  }
    d2_entity = DataMapper.repository(:default) { Intentity.new }
    d2_entity.attributes = { :id => Daitss.archive.uri_prefix + d1_entity.IEID, :original_name => d1_entity.PACKAGE_NAME, 
      :entity_id => d1_entity.ENTITY_ID, :volume =>  d1_entity.VOL, :issue => d1_entity.ISSUE, 
      :title => d1_entity.TITLE }

    # migrate package level (ingest, dissemination and withdraw) events
    d1_events = DataMapper.repository(:daitss1) { EVENT.all(:OID => d1_entity.IEID, :EVENT_TYPE => 'I') + 
    EVENT.all(:OID => d1_entity.IEID, :EVENT_TYPE => 'WO') + EVENT.all(:OID => d1_entity.IEID, :EVENT_TYPE => 'WA') }
    d1_events.each do |e|
      # migrate all d1 package events to d2 premis_events
      d2_e = DataMapper.repository(:default) { IntentityEvent.new }
      # temporary workaround: d1 event.DATE_TIME is stored as UTC but dm treat it as localtime
      # thus, we get the canonical datetime to convert the DATE_TIME to UTC .
      d2_e.attributes = { :id => e.ID, :idType => 'URI', :e_type => e.toD2EventType,
        :datetime => e.DATE_TIME.ctime, :event_detail => e.NOTE, :outcome => e.OUTCOME, :relatedObjectId => d2_entity.id }
      # has the package been withdrawn?
      if e.withdrawn?
        puts "withdrawn"
        withdrawn = true
      end

      # find the associated daitss I agent based on the event time
      d1_agent = @d1agent.find_agent(e.DATE_TIME)
      @agent = DataMapper.repository(:default) { PremisAgent.get(d1_agent.aid) }
      #@agent.premis_events << d2_e
      d2_e.premis_agent = @agent
      d2_events << d2_e
    end
             
    # migrate sip datafile records the from d1 to d2
    d1_datafiles = DataMapper.repository(:daitss1) { DATA_FILE.all(:IEID => d1_entity.IEID, :ORIGIN => 'DEPOSITOR') }
    d2_datafiles = Array.new
    total_size = 0
    d1_datafiles.each do |df|
      d2_df = DataMapper.repository(:default) { Datafile.new }
      is_sip_descriptor = df.ROLE.eql?("DESCRIPTOR_SIP")
      d2_df.attributes = { :id => df.DFID, :size => df.SIZE, :create_date => df.CREATE_DATE.ctime,
        :origin => df.ORIGIN, :original_path => df.PACKAGE_PATH, :creating_application => df.CREATOR_PROG,
        :is_sip_descriptor => is_sip_descriptor, :r0 => true, :rn => false, :rc => true }
      d2_datafiles << d2_df
      d2_entity.datafiles << d2_df 
      total_size += d2_df.size
      
      note = nil
      # record broken links discovered for this datafile during daitss1 processing
      sql = "SELECT BROKEN_LINKS from DISTRIBUTED where PARENT = '#{df.DFID}'"
      result = DataMapper.repository(:daitss1).adapter.select(sql)
      unless result[0].nil? || result[0].empty?
        @broken_links = result[0].split('|')
        # dump the xml output to the event note
        template = ERB.new File.new("views/broken_links.erb").read, nil, "%"
        note = template.result(binding)
        d2_e = DataMapper.repository(:default) { DatafileEvent.new }
        d2_e.attributes = { :id => df.DFID + '/event/broken_links', :idType => 'URI', :e_type => Daitss::BROKEN_LINKS,
          :datetime => Time.now, :outcome => 'success', :outcome_details => note, :relatedObjectId => d2_df.id }
        d2_e.premis_agent = @agent # use the last agent found on the package event
        d2_events << d2_e
      end
    end

    d1_copy = DataMapper.repository(:daitss1) { COPY.first(:IEID => @ieid)  }

    # migrate d1 disseminate event to d2 ops
    d1_opt_events  = d1_events.to_a + DataMapper.repository(:daitss1) { EVENT.all(:OID => d1_entity.IEID, :EVENT_TYPE => 'D')}.to_a
    
    DataMapper.repository(:default) do
      # create a package record for the ieid
      package = Package.new(:id => @ieid)
      @prj.packages << package

      # every package must be associated with a sip record.
      package.sip = Sip.new :name => d1_entity.PACKAGE_NAME
      package.sip.number_of_datafiles = d2_datafiles.size
      package.sip.size_in_bytes = total_size
      package.intentity = d2_entity

      # every ingested package should have an aip record			
      aip = Aip.new
      aip.package = package
      aip.xml = @d1_stud_descriptor
      # an aip may be located by the record in the copy table
      if d1_copy.nil?
        if !withdrawn
          raise "there is no record for this package in the DAITSS 1 COPY table, the package is not migrated"
        end
      else # the package has not been withdrawn and there is a COPY record
        copy = Copy.new(:aip => aip, :url => @storage_url + "/packages/" + @ieid, :sha1 => "", :md5 => d1_copy.MD5)
        aip.copy = copy
      end 
      package.transaction do
        raise "error saving package records #{package.inspect} #{package.errors.to_a}" unless package.save
        raise "cannot save intentity #{d1_entity.inspect} #{d1_entity.errors.to_a}" unless d2_entity.save
        #	raise "cannot save copy #{copy.inspect} #{copy.errors.to_a}" unless copy.save
        raise "cannot save aip #{aip.inspect} #{aip.errors.to_a}" unless aip.save
        d2_events.each {|e| raise "error saving event records #{e.inspect} #{e.errors.to_a}" unless e.save }
        # migrate d1 package events to d2 op events
        d1_opt_events.each do |e|
          # use the canonical datetime to convert the timezone into UTC .
          package.log  e.toD2OpsEventType, { :agent => @d1_op_agent, :timestamp => e.DATE_TIME.ctime, :notes =>  e.NOTE}
        end
      end
      
      package = nil
      aip = nil
      d1_copy = nil
    end
    d1_entity = nil
    d2_entity = nil
    d1_datafiles = nil
    d2_datafiles = nil
    d1_events = nil
    d2_events = nil
  end
  
end