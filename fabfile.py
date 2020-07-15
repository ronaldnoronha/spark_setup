# Import Fabric's API module
from fabric.api import sudo
from fabric.operations import reboot
from fabric2 import Connection, Config
from invoke import Responder
from fabric2.transfer import Transfer
import os

with open('./conf/master', 'r') as f:
    array = f.readline().split()
    remote_host = array[0]
    remote_port = array[1]
    user = array[2]
    host = array[3]

config = Config(overrides={'user': user})
c1 = Connection(host=host, config=config)
config2 = Config(overrides={'user': user, 'connect_kwargs': {'password': '1'}, 'sudo': {'password': '1'}})
c2 = Connection(host=remote_host, config=config2, gateway=c1)

all_connections = []
vms = ['192.168.122.137', '192.168.122.189', '192.168.122.208']

for i in vms:
    all_connections.append(Connection(host=i, config=config2, gateway=c1))

sudopass = Responder(pattern=r'\[sudo\] password:',
                     response='1\n',
                     )


def start_spark_cluster():
    c2.run('source /etc/profile && $SPARK_HOME/sbin/start-all.sh')
    # c2.run('cd /usr/local/spark && ./sbin/start-slaves.sh')


def stop_spark_cluster():
    c2.run('source /etc/profile && $SPARK_HOME/sbin/stop-all.sh')
    # c2.run('cd /usr/local/spark && ./sbin/stop-all.sh')


def spark_submit(size=10000):
    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--class org.apache.spark.examples.SparkPi '
        '--master spark://' + str(remote_host) + ':7077 '
        '--executor-memory 10g '
        './examples/jars/spark-examples_2.12-3.0.0.jar '+str(size))

def spark_submit_cluster(size=10000):
    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--class org.apache.spark.examples.SparkPi '
        '--master spark://' + str(remote_host) +':7077 '
        '--deploy-mode cluster '
        '--supervise '
        '--executor-memory 2g '
        './examples/jars/spark-examples_2.12-3.0.0-preview2.jar '+str(size))


def spark_submit_KMeans():
    c2.run('source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
            '--class org.apache.spark.examples.mllib.KMeansExample '
            '--master spark://' + str(remote_host) + ':7077 '
            '--executor-memory 2g '
            './examples/jars/spark-examples_2.12-3.0.0-preview2.jar')


def spark_test():
    # c2.run('source ~/.bashrc && env',replace_env=False)
    c2.run('source /etc/profile && env')


def restart_all_vms():
    for connection in all_connections:
        try:
            connection.sudo('shutdown -r now')
        except:
            continue
    try:
        c2.sudo('shutdown -r now')

    except:
        pass


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
        transfer.get('logs/log.csv', 'log' + str(counter) + '.csv')
        counter += 1


def start_monitors():
    for connection in all_connections:
        connection.run('nohup python3 ./monitor.py $1 >/dev/null 2>&1 &')


def stop_monitors():
    for connection in all_connections:
        connection.run('pid=$(cat logs/pid) && kill -SIGTERM $pid')


def transfer_file_to(filename):
    transfer = Transfer(c2)
    transfer.put(filename)


def example_spark_submit():
    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--class org.apache.spark.examples.mllib.KMeansExample '
        '--master spark://' + str(remote_host) + ':7077 '
        '--executor-memory 2g ~/spark_example_2.12-0.1.jar')

def example_uber():
    # temporary
    transfer = Transfer(c2)
    transfer.put('/Users/ronnie/Documents/spark_example/target/scala-2.12/spark_example_2.12-0.1.jar')

    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--class uber.KMeansUber '
        '--master spark://' + str(remote_host) + ':7077 '
                                                 '--executor-memory 2g ~/spark_example_2.12-0.1.jar')

def example_large_kmeans():
    # temporary
    transfer = Transfer(c2)
    transfer.put('/Users/ronnie/Documents/spark_example/target/scala-2.12/spark_example_2.12-0.1.jar')

    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--class com.example.kmeans.KMeansExample '
        '--master spark://' + str(remote_host) + ':7077 '
        '--executor-memory 2g ~/spark_example_2.12-0.1.jar')

def example_streaming():
    # temporary
    transfer = Transfer(c2)
    transfer.put('/Users/ronnie/Documents/StreamingModeSpark/target/scala-2.12/streamingmodespark_2.12-0.1.jar')

    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--class example.stream.StructureStreaming '
        '--master spark://' + str(remote_host) + ':7077 '
        '--deploy-mode cluster '
        '--executor-memory 100g '
        '~/streamingmodespark_2.12-0.1.jar')


def spark_test_ls():
    c2.run('ls')


def transfer_to_all(filename):
    for connection in all_connections:
        transfer = Transfer(connection)
        transfer.put(filename)

def start_nc():
    for connection in all_connections:
        connection.run('nohup nc -lk 9999 $1 >/dev/null 2>&1 &')


def example_streaming_kmeans():
    transfer = Transfer(c2)
    transfer.put('/Users/ronnie/Documents/spark_example/target/scala-2.12/spark_example_2.12-0.1.jar')

    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--class com.example.kmeans.KMeansExample '
        '--master spark://' + str(remote_host) + ':7077 '
                                                 '--executor-memory 2g ~/spark_example_2.12-0.1.jar')

def example_datagenerator():
    transfer = Transfer(c2)
    transfer.put('/Users/ronnie/Documents/StreamingModeSpark/target/scala-2.12/streamingmodespark_2.12-0.1.jar')

    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 '
        '--class example.stream.DataGenerator '
        '~/streamingmodespark_2.12-0.1.jar '
        '10000 '
        '~/100-bytes-lines.txt '
        '100'
    )

def example_kafka_trial():
    transfer = Transfer(c2)
    transfer.put('/Users/ronnie/Documents/spark_example/target/scala-2.12/spark_example_2.12-0.1.jar')
    # transfer.put('/Users/ronnie/Documents/datagenerator/kafka_producer_example.py')
    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0 '
        '--class example.stream.DirectKafkaWordCount '
        '~/spark_example_2.12-0.1.jar '
        'localhost:9092 '
        'consumer-group '
        'test'
    )

def start_kafka():
    c2.run('tmux new -d -s kafka')
    c2.run('tmux new-window')
    c2.run('tmux send -t kafka:0 /home/ronald/kafka_2.12-2.5.0/bin/zookeeper-server-start.sh\ '
           '/home/ronald/kafka_2.12-2.5.0/config/zookeeper.properties ENTER')
    c2.run('tmux send -t kafka:1 /home/ronald/kafka_2.12-2.5.0/bin/kafka-server-start.sh\ '
           '/home/ronald/kafka_2.12-2.5.0/config/server.properties ENTER')

def stop_kafka():
    c2.run('tmux kill-session -t kafka')

def example_streaming_kmeans():
    # transfer package
    transfer = Transfer(c2)
    transfer.put('/Users/ronnie/Documents/spark_example/target/scala-2.12/spark_example_2.12-0.1.jar')
    # transfer sample files
    c2.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0 '
        '--class example.stream.StreamingKMeansModelExample '
        '~/spark_example_2.12-0.1.jar '
        'localhost:9092 '
        'consumer-group '
        'test'
    )
