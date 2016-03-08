require 'fog/openstack'
require 'dragonfly'
require 'cgi'
require 'securerandom'

Dragonfly::App.register_datastore(:swift){ Dragonfly::SwiftDataStore }

module Dragonfly
  class SwiftDataStore

    # Exceptions
    class NotConfigured < RuntimeError; end

    SUBDOMAIN_PATTERN = /^[a-z0-9][a-z0-9.-]+[a-z0-9]$/

    def initialize(opts={})
      @container_name = opts[:container_name]
      @openstack_auth_url = opts[:openstack_auth_url]
      @openstack_username = opts[:openstack_username]
      @openstack_api_key = opts[:openstack_api_key]
      @storage_headers = {}
      @url_scheme = opts[:url_scheme] || 'http'
      @url_host = opts[:url_host]
      @root_path = opts[:root_path]
      @fog_storage_options = opts[:fog_storage_options] || {}
    end

    attr_accessor :container_name, :openstack_auth_url, :openstack_username, :openstack_api_key, :storage_headers, :url_scheme, :url_host, :root_path, :fog_storage_options

    def write(content, opts={})
      ensure_configured

      headers = {'Content-Type' => content.mime_type}
      headers.merge!(opts[:headers]) if opts[:headers]

      uid = opts[:path] || generate_uid(content.name || 'file')
      rescuing_socket_errors do
        content.file do |f|
          storage.put_object(container_name, full_path(uid), f, full_storage_headers(headers, content.meta))
        end
      end
      uid
    end

    def read(uid)
      ensure_configured
      Dragonfly.warn("Reading #{uid} #{full_path(uid)} from Swift");
      response =  storage.get_object(container_name, full_path(uid))
      [response.body, headers_to_meta(response.headers)]
    rescue Fog::Storage::OpenStack::NotFound => e
      nil
    end

    def destroy(uid)
      rescuing_socket_errors{ storage.delete_object(container_name, full_path(uid)) }
      nil
    rescue Fog::Storage::OpenStack::NotFound, Fog::Storage::OpenStack::Conflict => e
      Dragonfly.warn("#{self.class.name} destroy error: #{e}")
    end

    def url_for(uid, opts={})
      # TODO: see or this requires caching
      storage.public_url(container_name, full_path(uid))
    end

    def storage
      Excon.defaults[:ssl_verify_peer] = false
      @storage ||= begin
        storage = Fog::Storage.new(fog_storage_options.merge({
          provider: 'OpenStack',
          openstack_auth_url: @openstack_auth_url,
          openstack_username: @openstack_username,
          openstack_api_key: @openstack_api_key
        }).reject {|name, option| option.nil?})
        storage
      end
    end

    private

    def ensure_configured
      unless @configured
        [:openstack_auth_url, :openstack_username, :openstack_api_key, :container_name].each do |attr|
          raise NotConfigured, "You need to configure #{self.class.name} with #{attr}" if send(attr).nil?
        end
        @configured = true
      end
    end

    def generate_uid(name)
      "#{SecureRandom.uuid}/#{name}"
    end

    def full_path(uid)
      File.join *[root_path, uid].compact
    end

    def full_storage_headers(headers, meta)
      storage_headers.merge(meta_to_headers(meta)).merge(headers)
    end

    def headers_to_meta(headers)
      json = headers['X-Object-Meta-Data']
      if json && !json.empty?
        unescape_meta_values(Serializer.json_decode(json))
      elsif marshal_data = headers['X-Object-Meta-Data']
        Utils.stringify_keys(Serializer.marshal_b64_decode(marshal_data))
      end
    end

    def meta_to_headers(meta)
      meta = escape_meta_values(meta)
      {'X-Object-Meta-Data' => Serializer.json_encode(meta)}
    end

    def rescuing_socket_errors(&block)
      yield
    rescue Excon::Errors::SocketError => e
      storage.reload
      yield
    end

    def escape_meta_values(meta)
      meta.inject({}) {|hash, (key, value)|
        hash[key] = value.is_a?(String) ? CGI.escape(value) : value
        hash
      }
    end

    def unescape_meta_values(meta)
      meta.inject({}) {|hash, (key, value)|
        hash[key] = value.is_a?(String) ? CGI.unescape(value) : value
        hash
      }
    end
  end
end
