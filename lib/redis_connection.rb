class RedisConnection
  def self.redis=(server)
    case server
    when String
      if server =~ /redis\:\/\//
        redis = Redis.connect(:url => server, :thread_safe => true)
      else
        server, namespace = server.split('/', 2)
        host, port, db = server.split(':')
        redis = Redis.new(:host => host, :port => port,
          :thread_safe => true, :db => db)
      end
    end  
    #   namespace ||= :delayed_job

    #   @redis = Redis::Namespace.new(namespace, :redis => redis)
    # when Redis::Namespace
    #   @redis = server
    # else
    #   @redis = Redis::Namespace.new(:resque, :redis => server)
    # end
  end

  def self.redis
    return @redis if @redis
    self.redis = Redis.respond_to?(:connect) ? Redis.connect : "localhost:6379"  
  end  

  def self.reconnect
    raise "Not Active Redis Connection" unless @redis
    @redis.reconnect
  end

end

