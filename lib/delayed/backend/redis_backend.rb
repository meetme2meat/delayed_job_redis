module Delayed
  module Backend
    module RedisBackend
      class Job
        include ActiveModel 
        include Delayed::Backend::Base
        attr_reader :id,:priority,:attempts,:locked_at,:last_error,:failed_at,:locked_by,:queue
        attr_accessor :handler
        cattr_reader :queue_name 
        @@queue_name = :job_queue

        class <<  self

          def before_fork
          end
          
          def after_fork
            RedisConnection.reconnect
          end

          def ready_to_run(worker_name)
            RedisConnection.redis.lpop(queue_name)
          end
            
          def reserve(worker,max_run_time = Worker.max_run_time)
            ready_scope = self.ready_to_run(worker)
            if ready_scope
              new(JSON.parse(ready_scope))
            end
          end

          def count
            RedisConnection.redis.llen(queue_name)
          end
          alias_method :size,:count

          def all
            RedisConnection.redis.lrange(queue_name,0,-1).map do |job|
              new(JSON.parse(job))
            end
          end

          def destroy_all
            RedisConnection.redis.del(queue_name)
          end
          alias_method :delete_all,:destroy_all
        end

        def initialize(options)
          @id         = options["id"] || job_id
          @priority   = options["priority"]
          @attempts   = options["attempts"]
          @locked_at  = options["locked_at"]
          @last_error = options["last_error"]
          @failed_at  = options["failed_at"]
          @locked_by  = options["locked_by"]
          @queue      = options["queue"]
          if options["payload_object"]
            self.payload_object = options["payload_object"] 
          end
          self.handler ||= options[:handler]
          self
        end

        def job_id
          RedisConnection.redis.incr "job_id"
        end

        def save
          RedisConnection.redis.rpush(queue_name,serialize)
        end

        def destroy
          RedisConnection.redis.lrem queue_name,serialize
          self
        end

        def fail!
        end

        private
              
        def serialize
          to_json
        end
        
        def queue_name ## no doc
          queue || self.class.queue_name
        end
      end
    end
  end
end