require "active_support/hash_with_indifferent_access"
module Delayed
  module Backend
    module RedisBackend
      class Job
        #include ActiveModel::Validations
        include Delayed::Backend::Base
        attr_accessor :id,:run_at,:priority,:attempts,:locked_at,:last_error,:failed_at,:locked_by,:queue,:handler
        CLASS_STRING_FORMAT = /^CLASS\:([A-Z][\w\:]+)$/
        AR_STRING_FORMAT = /^AR\:([A-Z][\w\:]+)\:(\d+)$/
        class <<  self
          def before_fork ; end
          
          def after_fork
            RedisConnection.reconnect
          end

          def queue
            Worker.queues.shift || :default 
          end

          def ready_to_run_job(worker_name)
            jobs = []
            RedisConnection.redis.multi do
              ready_jobs = [] 
               jobs = (RedisConnection.redis.zrangebyscore queue,minimum_priority,maximum_priority,:limit => [0,1]).map do |serialize_job|
                job = new(JSON.parse(serialize_job))
                if job.ready_to_run(worker_name)
                  ready_jobs << serialize_job
                  job
                end
              end.compact
              RedisConnection.redis.zrem queue,ready_jobs if ready_jobs.length > 0
            end
            jobs
          end

          ## other method to come here 
          # def find(*args,queue)
          ##   if args.arity == 1 
          ##   detected_obj = RedisConnection.redis.lrange(queue,0,-1).detect do |job|
          ##      JSON.parse(i)["id"] == id
          ##   end
          ##   detected_obj unless detected_obj
          ##   new(JSON.parse(detected_obj))
          ## end 
          

          def find_by_id(*args)
            raise "Please specify conditions clauses" if args.arity == 1 
            conditions = args.shift 
            all(conditions[:queues]).detect do |job|
              job.id == args[0]
            end
          end

          # def find_all
          # end 

          # def where
          # end

          def minimum_priority
            Worker.min_priority || 0
          end

          def maximum_priority
            Worker.max_priority || 0
          end

    
          def reserve(worker,max_run_time = Worker.max_run_time)
            ready_scope = ready_to_run_job(worker)
            return nil unless ready_scope.length > 0
            ready_scope[0].update_attributes({:locked_at => db_time_now, :locked_by => worker.name})      
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
            sum = queues.uniq.flat_map { |q| RedisConnection.redis.zcard(q) }.sum
            queues.each { |q| RedisConnection.redis.del(q) }
            return sum
          end
          alias_method :delete_all,:destroy_all

          def clear_locks!(worker_name)
            RedisConnection.redis.multi do 
              locked_jobs = all.map do |job| 
                if job.has_lock?(worker_name)
                  job.to_json
                end
              end
              if locked_jobs.compact.length > 0
                unlocked_jobs = locked_jobs.map {|job| new(JSON.parse(job)).clear_locks }
                RedisConnection.redis.zrem queue,locked_jobs
                RedisConnection.redis.zadd unlocked_jobs
              end
            end
          end

          def db_time_now
            Time.zone.now.to_i
          end
        end

        def initialize(attributes={})
          attributes = ActiveSupport::HashWithIndifferentAccess.new(attributes)
          unless attributes.keys.include?("id")
            attributes.merge!({:id => job_id})
          end
          unless attributes.keys.include?("priority")
            attributes.merge!({:priority => nil})
          end
          attributes.each { |k,v| send(:"#{k}=",v) }
          self
        end
 

        def job_id
          RedisConnection.redis.incr "job_id"
        end

        def queue=(rqueue)
          @queue = rqueue.nil? ? :default : rqueue.to_sym 
        end

        def priority=(priority)
          @priority = priority.nil? ? self.class.minimum_priority : priority
        end

        def attempts=(attempts)
          @attempts = attempts.nil? ? 0 : attempts
        end

        def queue
          @queue = (@queue.nil? ? :default : @queue)
        end

        def attempts
          @attempts = (@attempts.nil? ?  0 : @attempts)
        end

        def update_attributes(args)
          raise "expected Hash but got #{args.class}"  unless args.class == Hash
          job_clone = self.clone
          args.map do |arg,val|
            send(:"#{arg}=",val)
          end
          RedisConnection.redis.multi do
            RedisConnection.redis.zrem queue,job_clone.send(:to_json)
            set_default_run_at
            RedisConnection.redis.zadd queue,priority,to_json
          end
          self
        end

        def save
          RedisConnection.redis.multi do 
            if RedisConnection.redis.zscore(queue,to_json)
              RedisConnection.redis.zrem queue,to_json
            end
            set_default_run_at
            RedisConnection.redis.zadd queue,priority,to_json
          end
        end

        def save!
          job = self.class.find_by_id(id,:conditions => {:queues => [queue]})
          RedisConnection.redis.multi do
            RedisConnection.redis.zrem queue,job.to_json if job 
            RedisConnection.redis.zadd queue,to_json
          end
        end


        def destroy
          RedisConnection.redis.zrem queue,to_json
        end

        def fail!
          RedisConnection.redis.multi do 
            RedisConnection.redis.zrem queue,to_json
            self.failed_at = self.class.db_time_now
            RedisConnection.redis.zadd queue,priority,to_json
          end
        end

        def inspect
          "#<Delayed::RedisBackend::Job id: #{@id}, run_at: #{format_the_time(@run_at)}, attempts: #{@attempts}, priority: #{@priority}, locked_at: #{format_the_time(@locked_at) || 'nil' }, failed_at: #{format_the_time(@failed_at) || 'nil'}, locked_by: #{@locked_by || 'nil'}, queue: #{@queue}, handler: #{@handler} >"
        end

        def ready_to_run(worker_name)
          (format_the_time(run_at) <= format_the_time(self.class.db_time_now) and (locked_at.nil? or format_the_time(locked_at) <  format_the_time(locked_at_time)) or locked_by == worker_name) and failed_at.nil?
        end
      
        def has_lock?(worker_name)
          locked_by == worker_name
        end

        def clear_locks
          unlock
          [self.priority,to_json]
        end

        private
      
        def to_json
          {id: @id,run_at: @run_at,priority: @priority,attempts: @attempts,locked_at: @locked_at,last_error: @last_error,failed_at: @failed_at,locked_by: @locked_by,queue: @queue,handler: @handler}.to_json
        end

        def locked_at_time
          self.class.db_time_now - self.class.max_run_time.to_i
        end

        def format_the_time(time)
          return nil if time.nil? 
          Time.at(time.to_i)
        end

        # def load(arg)
        #   case arg
        #     when CLASS_STRING_FORMAT then $1.constantize
        #     when AR_STRING_FORMAT then $1.constantize.find($2)
        #     else arg
        #   end
        # end

        # def dump(object)
        #   case object
        #   when Class,Module then class_to_string(object)
        #   when ActiveRecord::Base then ar_to_string(object)
        #   else object
        #   end
        # end

        # def class_to_string(object)
        #   "CLASS:#{object.name}"
        # end

        # def ar_to_string(object)
        #   "AR:#{object.class}:#{object.id}"
        # end
      end
    end
  end
end