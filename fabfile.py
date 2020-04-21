# Import Fabric's API module
from fabric.api import run, env, settings, remote_tunnel
from fabric.operations import reboot
from fabric2 import Connection, Config
from invoke import Responder
from fabric2.transfer import Transfer
import os

with open('./conf/master','r') as f:
    array = f.readline().split()
    remote_host = array[0]
    remote_port = array[1]
    user = array[2]
    host = array[3]

config = Config(overrides={'user': user})
c1 = Connection(host=host,config=config)
config2 = Config(overrides={'user': user,'connect_kwargs':{'password':'1'}, 'sudo':{'password':'1'}})
c2 = Connection(host=remote_host, config=config2, gateway=c1)

all_connections = []
vms = ['192.168.122.86','192.168.122.65','192.168.122.208']
for i in vms:
    all_connections.append(Connection(host=i, config=config2, gateway=c1))

sudopass = Responder(pattern=r'\[sudo\] password:',
                     response='1\n',
                     )


def start_spark_cluster():
    c2.run('source /etc/profile && cd /usr/local/spark/ && ./sbin/start-master.sh')
    c2.run('cd /usr/local/spark && ./sbin/start-slaves.sh')

def stop_spark_cluster():
    c2.run('cd /usr/local/spark && ./sbin/stop-all.sh')


def spark_submit():
    c2.run('cd /usr/local/spark && bin/spark-submit --class org.apache.spark.examples.SparkPi --master spark://'+str(remote_host)+
           ':7077 --executor-memory 2g ./examples/jars/spark-examples_2.12-3.0.0-preview2.jar 10000')

def spark_test():
    c2.run('source ~/.bashrc && env',replace_env=False)

def restart_all_vms():
    for connection in all_connections:
        # connection.reboot(wait=120,command='reboot',use_sudo=True)
        # connection.run('sudo reboot', watchers=[sudopass])
        connection.sudo('reboot')
