require 'redis'
require 'redis_connection'
require 'delayed_job'
require 'delayed/backend/redis_backend'

Delayed::Worker.backend = :redis_backend
