module Delayed
  module Backend
    module RedisBackend
      class Job
        include ActiveModel
        extend ActiveModel::Callbacks
        include Delayed::Backend::Base
        attr_accessor :id,:priority,:attempts,:locked_at,:last_error,:failed_at,:locked_by,:queue,:payload_object,:handler
        CLASS_STRING_FORMAT = /^CLASS\:([A-Z][\w\:]+)$/
        AR_STRING_FORMAT = /^AR\:([A-Z][\w\:]+)\:(\d+)$/
        class <<  self
          def before_fork ; end
          
          def after_fork
            RedisConnection.reconnect
          end

          def queue
            Worker.queues.shift || "default" 
          end

          def ready_to_run_job(worker_name)
            RedisConnection.redis.multi
            jobs = (RedisConnection.redis.zrangebyscore queue,minimum_priority,maximum_priority,:limit => [0,5]).map do |serialize_job|
              job = new(JSON.parse(serialize_job))
              if job.ready_to_run(worker_name)
                return serialize_job
              else 
                nil
              end  
            end.compact
            RedisConnection.redis.zrem queue,jobs
            RedisConnection.redis.exec
          end

          def minimum_priority
            Worker.minimum_priority || 0
          end

          def maximum_priority
            Worker.maximum_priority || 0
          end

          def reserve(worker,max_run_time = Worker.max_run_time)
            ready_scope = ready_to_run(worker)
            ready_scope.map do |serialize_job|
              new(JSON.parse(serialize_job))
            end
          end


          def count(*args)
            queues = ["default"] + (args || [])
            queues.inject(0) { |i,queue|  i += RedisConnection.redis.zcard(queue) ; i  }
          end
          alias_method :size,:count

          def all(*args)
            queues = ["default"] + (args || [])
            queues.uniq.flat_map { |q| RedisConnection.redis.zrange(q,0,-1) }.map  { |job| new(JSON.parse(job))}
          end

          def destroy_all(*args)
            queues = ["default"] + (args || [])
            queues.each { |q| RedisConnection.redis.del(q) }.count
          end
          alias_method :delete_all,:destroy_all

          def clear_locks!(worker_name)
            RedisConnection.redis.multi
            locked_jobs = all.map do |job| 
              if job.has_lock?(worker_name)
                job.to_json
              end
            end
            RedisConnection.redis.zrem queue,locked_jobs
            unlocked_jobs = locked_jobs.map {|job| new(JSON.parse(job)).clear_locks }
            RedisConnection.redis.zadd unlocked_jobs
            RedisConnection.redis.exec
          end

          def db_time_now
            Time.zone.now
          end
        end

        def initialize(attributes={})
          @attributes = attributes
          self
        end


        def save
          RedisConnection.redis.multi
          if RedisConnection.redis.zscore(queue,to_json)
            RedisConnection.redis.zrem queue,to_json
          end  
          set_default_run_at
          RedisConnection.redis.zadd queue,priority,to_json
          RedisConnection.redis.exec
        end

        def destroy
          RedisConnection.redis.zrem queue,to_json
        end

        def fail!
          RedisConnection.redis.multi
          RedisConnection.redis.zrem queue,to_json
          self.failed_at = self.class.db_time_now
          RedisConnection.redis.zadd queue,priority,to_json
          RedisConnection.redis.exec
        end


        def inspect
          "#<Delayed::RedisBackend::Job attempts: #{attempts} , priority: #{priority} , locked_at: #{locked_at} , failed_at: #{failed_at} , locked_by: #{locked_by} , queue: #{queue} , handler: #{handler} >"
        end

        def ready_to_run(worker_name)
          (run_at <= self.class.db_time_now and (locked_at.nil? or locked_at <  locked_at_time) or locked_by == worker_name) and failed_at.nil?
        end
      
        private
        
        def has_lock?(worker_name)
          job.locked_by == worker_name
        end

        def clear_locks
          unlock
          [self.priority,to_json]
        end

        def to_json
          {priority: priority,attempts: attempts,locked_at: locked_at,last_error: last_error,failed_at: failed_at,locked_by: locked_by,queue: queue,handler: handler }.to_json
        end  

        def locked_at_time
          self.class.db_time_now - self.class.max_run_time
        end

        def load(arg)
          case arg
            when CLASS_STRING_FORMAT then $1.constantize
            when AR_STRING_FORMAT then $1.constantize.find($2)
            else arg
          end
        end

        def dump(object)
          case object
          when Class,Module then class_to_string(object)
          when ActiveRecord::Base then ar_to_string(object)
          else object
          end
        end

        def class_to_string(object)
          "CLASS:#{object.name}"
        end

        def ar_to_string(object)
          "AR:#{object.class}:#{object.id}"
        end
      end
    end
  end
end