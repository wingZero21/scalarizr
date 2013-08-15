# -*- mode: ruby -*-
# vi: set ft=ruby :

boxes = {
  "ubuntu" => "ubuntu1204",
  "centos" => "centos63",
  "centos5" => "centos59",
  "debian" => "debian6",
  "amzn" => "amzn1303"
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
