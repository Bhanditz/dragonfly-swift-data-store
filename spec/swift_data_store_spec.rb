# encoding: UTF-8
require 'spec_helper'
require 'dragonfly/spec/data_store_examples'
require 'yaml'
require 'dragonfly/swift_data_store'

describe Dragonfly::SwiftDataStore do

  CONTAINER_NAME = 'test-gem'

  defaults = {
    openstack_auth_url: ENV.fetch('OPENSTACK_AUTH_URL'),
    openstack_username: ENV.fetch('OPENSTACK_USERNAME'),
    openstack_api_key: ENV.fetch('OPENSTACK_API_KEY'),
    storage_headers: {'x-amz-acl' => 'public-read'},
    container_name: CONTAINER_NAME,
    url_scheme: 'https'
  }

  # TODO fog-openstack mocking
  before(:each) do
    @data_store = Dragonfly::SwiftDataStore.new(
      defaults )
  end

  it_should_behave_like 'data_store'

  let (:app) { Dragonfly.app }
  let (:content) { Dragonfly::Content.new(app, "eggheads") }
  let (:new_content) { Dragonfly::Content.new(app) }

  describe "registering with a symbol" do
    it "registers a symbol for configuring" do
      app.configure do
        datastore :swift
      end
      app.datastore.should be_a(Dragonfly::SwiftDataStore)
    end
  end

  describe "write" do
    it "should use the name from the content if set" do
      content.name = 'doobie.doo'
      uid = @data_store.write(content)
      uid.should =~ /doobie\.doo$/
      new_content.update(*@data_store.read(uid))
      new_content.data.should == 'eggheads'
    end

    it "should work ok with files with funny names" do
      content.name = "A Picture with many spaces in its name (at 20:00 pm).png"
      uid = @data_store.write(content)
      uid.should =~ /A Picture with many spaces in its name \(at 20:00 pm\)\.png/
      new_content.update(*@data_store.read(uid))
      new_content.data.should == 'eggheads'
    end

    it "should allow for setting the path manually" do
      uid = @data_store.write(content, :path => 'hello/there')
      uid.should == 'hello/there'
      new_content.update(*@data_store.read(uid))
      new_content.data.should == 'eggheads'
    end
  end


  describe "not configuring stuff properly" do
    it "should require a container name on write" do
      @data_store.container_name = nil
      proc{ @data_store.write(content) }.should raise_error(Dragonfly::SwiftDataStore::NotConfigured)
    end

    it "should require an openstack auth url on write" do
      @data_store.openstack_auth_url = nil
      proc{ @data_store.write(content) }.should raise_error(Dragonfly::SwiftDataStore::NotConfigured)
    end

    it "should require an openstack username on write" do
      @data_store.openstack_username = nil
      proc{ @data_store.write(content) }.should raise_error(Dragonfly::SwiftDataStore::NotConfigured)
    end

    it "should require a api key on write" do
      @data_store.openstack_api_key = nil
      proc{ @data_store.write(content) }.should raise_error(Dragonfly::SwiftDataStore::NotConfigured)
    end

    it "should require a container name on read" do
      @data_store.container_name = nil
      proc{ @data_store.read('asdf') }.should raise_error(Dragonfly::SwiftDataStore::NotConfigured)
    end

    it "should require an openstack auth url on read" do
      @data_store.openstack_auth_url = nil
      proc{ @data_store.read('asdf') }.should raise_error(Dragonfly::SwiftDataStore::NotConfigured)
    end

    it "should require an openstack_username on read" do
      @data_store.openstack_username = nil
      proc{ @data_store.read('asdf') }.should raise_error(Dragonfly::SwiftDataStore::NotConfigured)
    end

    it "should require a api key on read" do
      @data_store.openstack_api_key = nil
      proc{ @data_store.read('asdf') }.should raise_error(Dragonfly::SwiftDataStore::NotConfigured)
    end
  end

  describe "headers" do
    before(:each) do
      @data_store.storage_headers = {'x-amz-foo' => 'biscuithead'}
    end

    it "should allow configuring globally" do
      @data_store.storage.should_receive(:put_object).with(CONTAINER_NAME, anything, anything,
        hash_including('x-amz-foo' => 'biscuithead')
      )
      @data_store.write(content)
    end

    it "should allow adding per-store" do
      @data_store.storage.should_receive(:put_object).with(CONTAINER_NAME, anything, anything,
        hash_including('x-amz-foo' => 'biscuithead', 'hello' => 'there')
      )
      @data_store.write(content, :headers => {'hello' => 'there'})
    end

    it "should let the per-store one take precedence" do
      @data_store.storage.should_receive(:put_object).with(CONTAINER_NAME, anything, anything,
        hash_including('x-amz-foo' => 'override!')
      )
      @data_store.write(content, :headers => {'x-amz-foo' => 'override!'})
    end

    it "should write setting the content type" do
      @data_store.storage.should_receive(:put_object) do |_, __, ___, headers|
        headers['Content-Type'].should == 'image/png'
      end
      content.name = 'egg.png'
      @data_store.write(content)
    end

    it "allow overriding the content type" do
      @data_store.storage.should_receive(:put_object) do |_, __, ___, headers|
        headers['Content-Type'].should == 'text/plain'
      end
      content.name = 'egg.png'
      @data_store.write(content, :headers => {'Content-Type' => 'text/plain'})
    end
  end

  describe "urls for serving directly" do

    before(:each) do
      @uid = 'some/path/on/s3'
    end

    it "should use the container domain" do
      @data_store.url_for(@uid).should include(CONTAINER_NAME)
    end
  end

  describe "meta" do
    it "uses the X-Object-Meta-Data header for meta" do
      uid = @data_store.write(content, :headers => {'X-Object-Meta-Data' => Dragonfly::Serializer.json_encode({'potato' => 44})})
      c, meta = @data_store.read(uid)
      meta['potato'].should == 44
    end

    it "works with non ascii character" do
      content = Dragonfly::Content.new(app, "hi", "name" => "こんにちは.txt")
      uid = @data_store.write(content)
      c, meta = @data_store.read(uid)
      meta['name'].should == 'こんにちは.txt'
    end
  end

end
