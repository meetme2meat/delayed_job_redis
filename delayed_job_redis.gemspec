Gem::Specification.new do |spec|
  spec.add_dependency   'redis', ['>= 3.0.2']
  spec.add_dependency   'json', ['>= 1.6.1']
  spec.add_dependency   'delayed_job', ['>= 2.1.0', '< 4']
  spec.authors        = ["Viren Negi"]
  spec.description    = 'Redis backend for Delayed::Job, originally authored by Viren Negi'
  spec.email          = ['meetme2meat@gmail.com']
  spec.files          = %w(LICENSE.txt README.rdoc Rakefile delayed_job_redis.gemspec)
  spec.files         += Dir.glob("lib/**/*.rb")
  spec.homepage       = 'http://github.com/meetme2meat/delayed_job_redis'
  spec.licenses       = ['MIT']
  spec.name           = 'delayed_job_redis'
  spec.require_paths  = ['lib']
  spec.summary        = 'Redis backend for DelayedJob'
  spec.version        = '0.1.0'
end
