if defined?(RedisBackend)
  class RedisBackend
    yaml_as "tag:ruby.yaml.org,2002:RedisBackend"
    def self.yaml_new(klass,tag,val)
      super
    end

    def to_yaml_properties
      super
    end
  end  
end