# -*- mode: ruby -*-
# vi: set ft=ruby :

boxes = {
  "ubuntu" => "ubuntu1204",
  "centos" => "centos63",
  "centos5" => "centos59",
  "amzn" => "amzn1303"
}

Vagrant::Config.run do |config|
  boxes.each do |name, box|
    config.vm.define name do |machine|
      machine.vm.box = box
      machine.vm.provision :chef_client do |chef|
        chef.chef_server_url = "http://sl5.scalr.net:4000"
        chef.node_name = "#{ENV['USER']}.scalarizr-#{machine.vm.box}-vagrant"
        chef.validation_client_name = "chef-validator"
        chef.run_list = ["recipe[vagrant_boxes]"]
        chef.validation_key_path = "validation.pem"
      end
    end
  end
end
