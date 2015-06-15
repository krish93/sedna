# 1. Introduction #

Sedna was developed and tested on Mac OS X and Fedora system, thanks to them first. :)

So, please make sure you are running Linux or Mac OS X on your servers before fellowing these installation steps.

# 2. Install #

## 2.1 Pre-require ##

All the servers you want to run Sedna should have the same directory structure, for example, every sever has directory '/home/sedna/sedna'.

Though Sedna is not a master-slaves architecture, there still should be a manage node which is used to start up cluster initially and stop cluster if needed. This management server should have ability to ssh to every other server without password.

A short steps:
```
> cd ~/.ssh
> ssh-keygen -t rsa
> scp id_rsa.pub nodei:~/.ssh/authorized_keys
> chmod 600 authorized_keys
```

### 2.1.1 Memcached Install ###

To run Sedna on your server, you need memcached to be installed first. In Sedna package, we have provided a memcached distribution, which can be automatically stared by our start-up script. However, before run start-up script, you need first compile memcached for your machine. Steps are:
```
  # cd ${Sedna_home}/memcached
  # ./configure
  # make
```
Before do these install steps, make sure your machine has installed fellowing programs:
  * gcc
  * make
  * libevent

### 2.1.2 Jetty Server Install ###

It's quite simple, only need make sure is the web port(default 10010) has not been used.

### 2.1.3 ZooKeeper Install ###

Install ZooKeeper in distributed mode is necessary for a serious Sedna deployment. If you want run Sedna as a test system on a few servers, a stand alone zookeeper installation is also fine.

After Installing ZooKeeper, please remember the ZooKeeper cluster server list and their port, which we need to configure Sedna before start it up.

## 2.2 System Modify for Linux ##

### 2.2.1 Hosts ###

We highly recommend users modify /etc/hosts file in servers. As in Sedna we use InetAddress.getHostAddr() to get local machine's IP address and use Jetty to start a local web server. So the best way to avoid not necessary problems is modify /etc/hosts file, and try to find every machine by their name, like nodek.

Remove 127.0.0.1 or :::1's name if their name columns include host name. Add a new line like this:

  * 192.168.1.15 node1

### 2.2.2 file-max ###

The max number of files a process can open is limited in nearly all the linux distribution. You can check them using:
```
> ulimit -n
```
Usually 1024 in Fedora or 256 in Mac OS X. A large Sedna cluster may need more sockets than these numbers, so we recommend users to modify this value.  The best way is change files in /etc/security/limits.conf, add a row in the last line of that file:
```
* - nofile 1000000
```
reboot and that value will begin to affect your system.

It's a little different when you are working on Mac OS X, change file in /etc/launchd.conf, and a row in this file( usually this file is not exist! so you need to create it first)
```
limit maxfiles 1000000 1000000
```

## 2.3 Configuration ##

### 2.3.1 Configuration File ###

Configuration in Sedna includes:
```
move_data_threads=10
data_movein_thread_interval=180000

memcached_server_port=11211
tcp_server_port=11212
tcp_idle_timeout=1000000

update_vtable_lease_time=300000
update_vtable_min_least_time=10000

virtual_node_number=100

zookeeper_servers=192.168.1.100:2181

remove_i_change_vnode_interval=600000

sedna_log_dir="../logs"
sedna_web_port=10010

read_quorum=2
write_quorum=2

thread_pool_size=100
```

Most value should be kept as default value, however, some of them must modify correctly if you want to deploy your own Sedna cluster.
```
memcached_server_port = 11211 //the port memcached client listens at
virtual_node_number = 100 // typically  64*(your_server_numbers). if i have 10 servers, i should set this value to 640
zookeeper_servers=node1:2181,node2:2181 //ZooKeeper cluster server list
sedna_web_port = 10010 // make sure this port doesn't used
tcp_server_port = 11212 // port that Sedna used, make sure it does not be used by other programs
```

### 2.3.2 Nodes File ###

file conf/nodes contains all the node included in our Sedna cluster.
```
node1
node2
node3
node4
...
```
You can use ip or host name to denote these servers.

## 2.4 Begin Sedna ##

After configuration and prepare work, we can start our Sedna cluster now.

```
cd ${Sedna_Home}
bin/start-sedna.sh
```


## 2.5 Stop Sedna ##

```
cd ${Sedna_Home}
bin/stop-sedna.sh
```

If your Linux distribution does not support 'pkill' command, you should stop memcached instance manually on every server. Usually, memcached would not have serious problems, so if you can not stop memcached, please fell free.

# 3. Usage #

## 3.1 Data Read ##

## 3.2 Data Write ##

# 4. Benchmark #

# 5. Technique Paper #

# 6. Others #

Add your content here.  Format your content with:
  * Text in **bold** or _italic_
  * Headings, paragraphs, and lists
  * Automatic links to other wiki pages