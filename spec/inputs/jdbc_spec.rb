require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/jdbc"
require "jdbc/derby"
require "timecop"

describe "jdbc" do
  let(:mixin_settings) { {"jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver", "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"} }

  before :each do
    Jdbc::Derby.load_driver
  end

  it "should register and tear down" do
    settings = {"statement" => "SELECT 1 as col1 FROM SYSIBM.SYSDUMMY1"}
    plugin = LogStash::Plugin.lookup("input", "jdbc").new(mixin_settings.merge(settings))
    expect { plugin.register }.to_not raise_error
    expect { plugin.teardown }.to_not raise_error
  end

  it "should retrieve params correctly from Event" do
    settings = {"statement" => "SELECT :num_param as num_param FROM SYSIBM.SYSDUMMY1", "parameters" => {"num_param" => 10} }
    plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
    plugin.register
    q = Queue.new
    plugin.run(q)
    insist { q.size } == 1
    insist { q.pop['num_param'] } == settings['parameters']['num_param']
    plugin.teardown
  end

  it "should properly schedule" do
    settings = {"statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1", "schedule" => "* * * * *"}
    plugin = LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings))
    plugin.register
    q = Queue.new
    Timecop.travel(Time.new(2000))
    Timecop.scale(60)
    runner = Thread.new do
      plugin.run(q)
    end
    sleep 3
    runner.kill
    runner.join
    insist { q.size } == 2
    plugin.teardown
    Timecop.return
  end
end
