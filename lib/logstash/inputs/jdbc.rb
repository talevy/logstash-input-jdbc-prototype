# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

# Read in rows from a database
class LogStash::Inputs::Jdbc < LogStash::Inputs::Base
  config_name "jdbc"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain" 

  # The jdbc connection string
  config :conn_str, :validate => :string, :required => true

  # The query
  config :query, :validate => :string, :required => true

  # The database user to use for the connection
  config :user, :validate => :string, :default => "#{Etc.getlogin}"

  # The user's password
  config :password, :validate => :string, :default => ""

  # Schedule
  config :cron_schedule, :validate => :string

  public

  def register
    require "rufus-scheduler"
    require "etc"
    require 'jdbc/postgres'
    Jdbc::Postgres.load_driver
    @conn = JavaSql::DriverManager.getConnection(@conn_str, @user, @password)
  end # def register

  def run(queue)
    if @cron_schedule
      scheduler = Rufus::Scheduler.new
      scheduler.cron @cron_schedule do
        execute_query(queue)
      end
    else
      execute_query(queue)
    end
  end # def run

  def teardown
    @conn.close
  end # def teardown

  private

  module JavaSql
    include_package 'java.sql'
  end

  def execute_query(queue)
    stm = @conn.createStatement
    rs = stm.executeQuery(@query)
    meta = rs.getMetaData
    col_count = meta.getColumnCount
    while (rs.next) do
      row_hash = Hash[(1..col_count).map { |col| [meta.getColumnName(col), rs.getObject(col)] }]
      event = LogStash::Event.new(row_hash)
      decorate(event)
      queue << event
    end
    rs.close
    stm.close
  end
end # class LogStash::Inputs::Jdbc
