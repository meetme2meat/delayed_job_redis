module RedisConnection
  class << self
    attr_accessor :connection
  end
    
  def self.reconnect
    connection.reconnect
  end
end