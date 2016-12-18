#!/bin/sh
export DISPLAY=:1
#ROOTDIR=$( cd "$( dirname "$0" )" && pwd )
ROOTDIR=/tmp
INSTANCE=$2
WORKINGDIR=$ROOTDIR/$INSTANCE

#JAVA_HOME=/algoeye/jdk1.8.0_05
#PATH=$JAVA_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
#JAVA=$JAVA_HOME/bin/java
#COMMAND="$JAVA -cp jts.jar:total.2013.jar:IBController.jar ibcontroller.IBGatewayController IBController.ini"
COMMAND="bash /opt/IBController/IBControllerGatewayStart.sh"
PIDFILE=$WORKINGDIR/ibgateway.pid
LOGFILE=$WORKINGDIR/ibgateway.log
IBC_INI=/home/david/IBController/IBController.ini

stop()
{
   #if kill -0 `cat $PIDFILE` >/dev/null 2>&1; then 
   #    echo "Stopping gateway $INSTANCE with pid: `cat $PIDFILE`\n" >> $LOGFILE;
   #    kill -9 `cat $PIDFILE`
   #fi
   # Mata el proceso que tiene pillado este fichero que es el java que ejecuta el IBGateway
   echo "Stopping gateway $INSTANCE \n" >> $LOGFILE;
   pgrep -f "$IBC_INI"  | xargs kill -9
}

restart()
{
       stop
       echo "Starting gateway $INSTANCE in $WORKINGDIR with pid: $$\n" >> $LOGFILE;
       echo $$ > $PIDFILE;
       cd $WORKINGDIR;
       exec $COMMAND 1>>$LOGFILE 2>&1
}

status()
{
	echo "PIDFILE=" $PIDFILE "WORKINGDIR=" $WORKINGDIR
	
}

case $1 in
     start)
       restart
       ;;
     restart)
       restart
       ;;
	 status)
		status
		;;
     stop)  
       stop
       ;;
     *)  
       echo "usage: gw.sh {start|stop|restart} <instance>" ;;
esac
exit 0
