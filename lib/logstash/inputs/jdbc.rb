# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/jdbc"

# Read in rows from a database
class LogStash::Inputs::Jdbc < LogStash::Inputs::Base
  include LogStash::PluginMixins::Jdbc
  config_name "jdbc"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain" 

  # Statement to execute
  # To use parameters, use named parameter syntax, for example "SELECT * FROM MYTABLE WHERE ID = :id"
  config :statement, :validate => :string, :required => true

  # Hash of query parameter, for example `{ "id" => "id_field" }`
  config :parameters, :validate => :hash, :default => {}

  # currently only supports UTC
  config :timezone, :validate => :string, :default => "UTC"

  # Schedule of when to periodically run statement, in Cron format
  config :schedule, :validate => :string

  public

  def register
    require "rufus-scheduler"
    prepare_jdbc_connection()
  end # def register

  def run(queue)
    if @schedule
      @scheduler = Rufus::Scheduler.new
      @scheduler.cron @schedule do
        execute_query(queue)
      end
    else
      execute_query(queue)
    end
  end # def run

  def teardown
    if @scheduler
      @scheduler.shutdown
    end
    close_jdbc_connection()
  end # def teardown

  private
  def execute_query(queue)
    # update default parameters
    @parameters['sql_last_start'] = @sql_last_start
    execute_statement(@statement, @parameters) do |row|
      event = LogStash::Event.new(row)
      decorate(event)
      queue << event
    end
  end
end # class LogStash::Inputs::Jdbc
