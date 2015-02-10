require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/jdbc"

describe LogStash::Inputs::Jdbc do
  it "should register" do
    settings = {"query" => "select * from hello", "conn_str" => "jdbc:postgresql://localhost/tal"}
    plugin = LogStash::Inputs::Jdbc.new(settings)
    plugin.register
    q = Queue.new
    plugin.run(q)
    while not q.empty?
      p q.pop
    end
  end
end
