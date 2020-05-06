#!/bin/bash
dir=$(cd `dirname $0`;pwd)
source $dir/conf
pid=`ps -ef| grep "$jarpath/$jarpack" | grep -v grep |awk '{print $2}'`
if [ "$pid"x != "x" ];then
   kill  $pid
fi
sleep 1s
ret=`ps -ef | grep "$jarpath/$jarpack" | grep -v grep`
sleep 10s
if [ "$ret"x = "x" ];then
    echo "you have stop the your service $jarpack now!"
else
    pid=`ps -ef| grep "$jarpath/$jarpack" | grep -v grep |awk '{print $2}'`
    if [ "$pid"x != "x" ];then
        echo "you service can't be stoped,so kill -9 your service! "
        kill -9 $pid
    fi
fi
