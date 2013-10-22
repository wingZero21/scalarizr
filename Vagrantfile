# -*- mode: ruby -*-
# vi: set ft=ruby :

boxes = {
  "ubuntu" => "opscode-ubuntu-12.04",
  "ubuntu1004" => "opscode-ubuntu-10.04",
  "centos" => "opscode-centos-6.4",
  "centos5" => "opscode-centos-5.9",
  "amzn" => "dummy",
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

      if name == "windows"
        machine.vm.guest = :windows
        machine.vm.network :forwarded_port, guest: 5985, host: 5985, name: "winrm"
        machine.vm.network :forwarded_port, guest: 3389, host: 3390, name: "rdp"
        machine.winrm.username = "vagrant"
        machine.winrm.password = "vagrant"
        machine.vm.network :private_network, ip: "192.168.33.10" 
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
