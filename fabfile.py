# Import Fabric's API module
from fabric.api import sudo
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
vms = ['192.168.122.137','192.168.122.189','192.168.122.208']

for i in vms:
    all_connections.append(Connection(host=i, config=config2, gateway=c1))

sudopass = Responder(pattern=r'\[sudo\] password:',
                     response='1\n',
                     )


def start_spark_cluster():
    c2.run('source /etc/profile && $SPARK_HOME/sbin/start-all.sh')
    # c2.run('cd /usr/local/spark && ./sbin/start-slaves.sh')

def stop_spark_cluster():
    c2.run('source /etc/profile && $SPARK_HOME/stop-all.sh')
    # c2.run('cd /usr/local/spark && ./sbin/stop-all.sh')


def spark_submit():
    c2.run('source /etc/profile && cd $SPARK_HOME && bin/spark-submit --class org.apache.spark.examples.SparkPi --master spark://'+str(remote_host)+
           ':7077 --executor-memory 2g ./examples/jars/spark-examples_2.12-3.0.0-preview2.jar 10000')

def spark_test():
    # c2.run('source ~/.bashrc && env',replace_env=False)
    c2.run('source /etc/profile && env')

def restart_all_vms():
    for connection in all_connections:
        try:
            connection.sudo('shutdown -r now')
        except:
            continue

def transfer_monitor():
    for connection in all_connections:
        connection.run('rm monitor.py')
        connection.run('rm -rf logs')
        transfer = Transfer(connection)
        transfer.put('monitor.py')
        connection.run('mkdir logs')

def transfer_logs_out():
    counter = 1
    for connection in all_connections:
        transfer = Transfer(connection)
        transfer.get('logs/log.csv','log'+str(counter)+'.csv')
        counter+=1

def start_monitors():
    for connection in all_connections:
        connection.run('nohup python3 ./monitor.py $1 >/dev/null 2>&1 &')

def stop_monitors():
    for connection in all_connections:
        connection.run('pid=$(cat logs/pid) && kill -SIGTERM $pid')



