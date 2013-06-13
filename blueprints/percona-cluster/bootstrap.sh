#!/bin/bash


template="ubuntu"
release="precise"
ssh_priv="/root/.ssh/id_rsa"
ssh_pub="${ssh_priv}.pub"


host_packages() {
    apt_update_stamp="/var/lib/apt/periodic/update-success-stamp"
    if test -e $apt_update_stamp && (($(date +%s) - $(stat --format="%Y" $apt_update_stamp) >= 86400)); then
        apt-get update
    fi
    apt-get install -y -q lxc
}

host_ssh_keypair() {
    if ! test -e $ssh_priv; then
        ssh-keygen -t rsa -N '' -f $ssh_priv
    fi
}

container_created() {
    # container should be created
    container=$1
    if ! lxc-list | grep -q $container; then
        echo "Create LXC container $container"
        lxc-create --name $container --template $template -- --release $release 1>&2
    fi
}

container_started() {
    container=$1
    # container should be started
    if ! lxc-info --name $container 2>&1 | grep -qs RUNNING; then
        echo "Start LXC container $container"
        lxc-start --name $container --daemon
    fi
}

container_stopped() {
    container=$1
    # container should be stopped
    if lxc-info --name $container 2>&1 | grep -qs RUNNING; then
        echo "Stopping LXC container $container"
        lxc-stop --name $container
    fi
}

container_clone() {
    old=$1
    new=$2
    echo "Cloning LXC container $old -> $new"
    lxc-clone -o $old -n $new
    for dir in dhcp dhcp3; do
        leases_file="/var/lib/lxc/$new/rootfs/var/lib/$dir/dhclient.eth0.leases"
        rm -f $leases_file
    done
}

container_authorized_keys() {
    container=$1
    # ssh public key should be presented
    authorized_keys="/var/lib/lxc/$container/rootfs/root/.ssh/authorized_keys"
    echo "Manage file $authorized_keys"
    if ! test -e $(dirname $authorized_keys); then
        mkdir -p $(dirname $authorized_keys)
        chmod 700 $(dirname $authorized_keys)
    fi
    cp $ssh_pub $authorized_keys
    chmod 400 $authorized_keys
}

host_mapping_in_hosts_file() {
    container=$1
    # /etc/hosts should contains fresh mappings
    for dir in dhcp dhcp3; do
        leases_dir="/var/lib/lxc/$container/rootfs/var/lib/$dir"
        if test -e $leases_dir; then
            echo "Manage /etc/hosts mapping $ipaddr -> $container"
            leases_file="$leases_dir/dhclient.eth0.leases"
            _wait_expr test -e $leases_file
            _wait_expr grep -q 'fixed-address' $leases_file
            ipaddr=$(grep 'fixed-address' $leases_file | tail -1 | awk '{ print $2}' | sed s/.$//)
            _put_hosts_entry $ipaddr $container
            ipaddr=$(grep 'dhcp-server-identifier' $leases_file | tail -1 | awk '{ print $3 }' | sed s/.$//)
            _put_hosts_entry $ipaddr 'salt' "/var/lib/lxc/$container/rootfs/etc/hosts"
            return
        fi
    done
    echo "Failed to find dhclient.eth0.leases file in container $container" >2
    return 1
}

percona_apt() {
    key_id="CD2EFD2A"
    key_server="hkp://keys.gnupg.net"
    repo_file="/etc/apt/sources.list.d/percona.list"
    if ! apt-key list | grep -q $key_id; then
        apt-key adv --keyserver $key_server --recv $key_id
    fi
    if ! test -e $repo_file; then
        echo "deb http://repo.percona.com/apt $release main" > $repo_file
        apt_get_update_repo $(basename $repo_file) 
    fi
}

percona_cluster_installed() {
    export DEBIAN_FRONTEND=noninteractive
    export DEBIAN_PRIORITY=critical
    apt-get install -y -q percona-xtradb-cluster-server-5.5 percona-xtradb-cluster-client-5.5    
}

percona_cluster_configure() {
    cat > /etc/mysql/my.cnf <<EOC
[mysqld]
datadir = /var/lib/mysql
default_storage_engine=InnoDB
binlog_format=ROW

innodb_locks_unsafe_for_binlog=1
innodb_autoinc_lock_mode=2

wsrep_provider=/usr/lib/libgalera_smm.so
wsrep_cluster_address=gcomm://node-1,node-2,node-3
#wsrep_cluster_address=gcomm://
wsrep_slave_threads=16

wsrep_sst_method=xtrabackup
wsrep_sst_auth=root:

#wsrep_sst_method=rsync
EOC
}

apt_get_update_repo() {
    repo=$1
    apt-get update -o Dir::Etc::sourcelist="sources.list.d/$repo" \
                    -o Dir::Etc::sourceparts="-" \
                    -o APT::Get::List-Cleanup="0"
}

container_bootstrap() {
    percona_apt
    percona_cluster_installed
    percona_cluster_configure
    if [ $(hostname) = "node-1" ]; then
        /etc/init.d/mysql restart --wsrep-cluster-address="gcomm://"
    else
        /etc/init.d/mysql restart
    fi
}

container_create_bootstrap_script() {
    container=$1
    script_path="/var/lib/lxc/$container/rootfs/etc/rc.local"
    cp $script_path "$script_path.bak"
    echo "#!/bin/bash" > $script_path
    echo "release=$release" >> $script_path
    declare -f 'percona_apt' >> $script_path
    declare -f 'apt_get_update_repo' >> $script_path
    declare -f 'percona_cluster_installed' >> $script_path
    declare -f 'percona_cluster_configure' >> $script_path
    declare -f 'container_bootstrap' >> $script_path
    echo 'container_bootstrap' >> $script_path
    chmod +x $script_path
    cat $script_path
}

container_remove_bootstrap_script() {
    container=$1
    script_path="/var/lib/lxc/$container/rootfs/etc/rc.local"
    cp "$script_path.bak" $script_path
    chmod +x $script_path
}

_wait_expr() {
    for _ in $(seq 0 9); do
        if $@; then
            return
        fi
        sleep 1
    done
    echo "wait condition timeouted: $@"
    return 1
}

_put_hosts_entry() {
    ipaddr=$1
    host=$2
    hosts_file=${3:-/etc/hosts}
    # set or update ipaddr -> hostname mapping in hosts file
    sed -i "s/^.*$host$/$ipaddr $host/" $hosts_file
    if ! grep -q $host $hosts_file; then
        echo "$ipaddr $host" >> $hosts_file
    fi
}

_main() {
    host_packages
    host_ssh_keypair
    container_created "node-1"
    container_authorized_keys "node-1"
    container_create_bootstrap_script "node-1"
    container_started "node-1"
    host_mapping_in_hosts_file "node-1"
    container_stopped "node-1"
    container_remove_bootstrap_script "node-1"
    for i in $(seq 2 3); do
        container_clone "node-1" "node-$i"
        container_started "node-$i"
        host_mapping_in_hosts_file "node-$i"
    done
}

#_main
