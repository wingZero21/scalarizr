# -*- mode: ruby -*-
# vi: set ft=ruby :

boxes = {
  "ubuntu" => "ubuntu-12.04",
  "ubuntu1004" => "ubuntu-10.04",
  "centos" => "centos-6.4",
  "centos5" => "centos-5.9",
  "debian" => "debian-6",
  "amzn" => "amzn-13.03",
  "windows" => "windows-2008r2"
}

Vagrant.configure("2") do |config|
  boxes.each do |name, box|
    config.vm.define name do |machine|
      machine.vm.box = box
      machine.vm.provision :chef_solo do |chef|
        chef.cookbooks_path = "cookbooks/cookbooks"
        chef.add_recipe "vagrant_boxes::scalarizr"
      end

      if name == "amzn"
        machine.vm.provider :aws do |aws|
          aws.access_key_id = ENV['EC2_ACCESS_KEY']
          aws.secret_access_key = ENV['EC2_SECRET_KEY']
          aws.keypair_name = "vagrant"
          aws.ssh_private_key_path = ENV['EC2_VAGRANT_SSH_KEY']
          aws.ssh_username = "root"
          aws.ami = "ami-ccc1a4a5"
        end      
      end
    end
  end
end
