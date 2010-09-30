require 'daitss1/daitss1'

def setup
	Daitss1.setup
	DataMapper.setup(:daitss2, "postgres://daitss:topdrawer@localhost/daitss2")
end

def migrate ieid
	DataMapper.repository(:daitss1) do
		@d1_entity = INT_ENTITY.get(ieid)   
		@d1_datafiles = DATA_FILE.all(:IEID => @entity.IEID, :ORIGIN => 'DEPOSITOR')
		@d1_events = EVENT.all(:OID => @entity.IEID)
	end
	
	d2_datafiles = Array.new
	@d1_datafiles.each do |df|
		DataMapper.repository(:daitss2) { d2_df = Datafile.new }
		df_df.attributes = { :id => df.DFID, :size => df.SIZE, :create_date => df.CREATE_DATE,
		:origin => df.ORIGIN, :original_path => df.PACKAGE_PATH + df.FILE_TITLE, 
		:creating_application => df.CREATOR_PROG }
		d2_datafiles << d2_df
	end
	
	DataMapper.repository(:daitss2) { @d2_entity = Intentity.new }
	@d2_entity.attributes = { :id => @d1_entity.IEID, :original_name => @d1_entity.PACKAGE_NAME, 
	  :entity_id => @d1_entity.ENTITY_ID, :volume =>  @d1_entity.VOL, :issue => @d1_entity.ISSUE, 
	  :title => @d1_entity.TITLE }
	
    d2_datafiles.each { |d2_df| @d2_entity.datafiles << d2_df }

end