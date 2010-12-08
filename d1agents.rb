require 'singleton'

AGENTS = [
	['info:fcla/daitss/v1.?.?', '2005-10-01', '2007-08-15' ], 
	['info:fcla/daitss/v1.2.4', '2007-08-15', '2007-10-31' ],
	['info:fcla/daitss/v1.2.5', '2007-10-31', '2008-03-13' ],
	['info:fcla/daitss/v1.5.2', '2008-03-13', '2008-05-09' ],
	['info:fcla/daitss/v1.5.4', '2008-05-09', '2008-05-12' ],
	['info:fcla/daitss/v1.5.5', '2008-05-12', '2008-09-16' ],
	['info:fcla/daitss/v1.5.7', '2008-09-16', '2008-11-21' ],
	['info:fcla/daitss/v1.5.8', '2008-11-21', '2009-01-07' ],
	['info:fcla/daitss/v1.5.9', '2009-01-07', '2009-04-02' ],
	['info:fcla/daitss/v1.5.10', '2009-04-02', '2009-04-08' ],
	['info:fcla/daitss/v1.5.11', '2009-04-08', '2009-11-13' ],
	['info:fcla/daitss/v1.5.12', '2009-11-13', '2010-01-07' ],
	['info:fcla/daitss/v1.5.13', '2010-01-07', '2010-05-25' ],
	['info:fcla/daitss/v1.5.14', '2010-05-25', '2010-09-01' ],
	['info:fcla/daitss/v1.5.15', '2010-09-01', '2010-10-31' ]
]

class D1Agent
  attr_accessor :aid
  attr_accessor :start_time
  attr_accessor :end_time

  def initialize(aid, start_time, end_time)
	@aid = aid
    @start_time = DateTime.parse(start_time)
    @end_time = DateTime.parse(end_time)
  end
end

class D1Agents
  include Singleton
  attr_accessor :agents

  def initialize
    @agents = Array.new
    for i in 0..14
       agent = D1Agent.new(AGENTS[i][0], AGENTS[i][1], AGENTS[i][2])
       @agents << agent
    end
  end

  def find_agent ingest_time
    agent = nil
    @agents.each do |a|
       if (ingest_time < a.end_time && ingest_time >= a.start_time)
         agent = a
         break
       end
    end
    agent
  end
end