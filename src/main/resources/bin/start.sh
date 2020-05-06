#!/bin/bash
dir=$(cd `dirname $0`;pwd)
source $dir/conf
ret=`ps -ef | grep "$jarpath/$jarpack" | grep -v grep`
if [ "$ret"x == "x" ];then
cd $jarpath
   cmd="$path/java $JAVA_OPTS -Xbootclasspath/a:$jarpath/$Xbootclasspath  -jar $jarpath/$jarpack $sys_after"
   echo "cmd is : "$cmd
   nohup $path/java $JAVA_OPTS -Xbootclasspath/a:$jarpath/$Xbootclasspath  -jar $jarpath/$jarpack  $sys_after >$jarpath/server.log 2>&1 &
fi
sleep 1s
ret=`ps -ef | grep "$jarpath/$jarpack" | grep -v grep`
if [ "$ret"x != "x" ];then
     echo "you have start the your service $jarpack now!"
 else
     echo "Please check your conf! and re-excute the start.sh"
 fi
