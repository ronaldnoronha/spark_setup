cat ./conf/master | while read line
do
  array=($line)
  ipaddr=${array[0]}
  port=${array[1]}
  user=${array[2]}
  addr=${array[3]}
  ssh -Nf -L $port:$ipaddr:8080 $user@$addr
done

fab start_spark_cluster

fab spark_submit

#fab stop_spark_cluster



