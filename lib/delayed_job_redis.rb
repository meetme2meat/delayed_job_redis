require 'redis'
require 'redis_connection'
require 'delayed_job'
require 'delayed/backend/redis'

RedisConnection.connection = Redis.new
Delayed::Worker.backend = :redis
