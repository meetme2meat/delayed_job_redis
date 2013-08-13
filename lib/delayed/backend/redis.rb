module Delayed
  module Backend
    module Redis
      class Job
        include Delayed::Backend::Base
        attr_reader :handler,:priority,:attempts,:locked_at,:last_error,:failed_at,:locked_by,:queue

        def intialize(options)
          @handler    = options[:handler]
          @priority   = options[:priority]
          @attempts   = options[:attempts]
          @locked_at  = options[:locked_at]
          @last_error = options[:last_error]
          @failed_at  = options[:failed_at]
          @locked_by  = options[:locked_by]
          @queue      = options[:queue]
        end

        def self.before_fork
        end
        
        def self.after_fork
          RedisConnection.reconnect
        end

        def self.ready_to_run(worker_name)
          active_connection.redis.lpop(self.class.to_s)
        end
          
        def self.reserve(worker,max_run_time = Worker.max_run_time)
          ready_scope = self.ready_to_run(worker)
        end

        def save
          active_connection.redis.lpush(queue_name,serialize)
        end

        private
        
        def active_connection
          @active_connection ||= RedisConnection.connection 
        end
          
        def serialize
          to_json
        end
        
        def queue_name ## no doc
          queue || self.class.to_s
        end
      end
    end
  end
end