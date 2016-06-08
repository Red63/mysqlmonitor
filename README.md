# 项目说明

>     本项目中子模块retail-canaldata-consumer为从canal-Server中采集mysql增量日志(Binlog)到kafka中。本模块可单独作为java进程运行，也可以集成在web项目中使用。具体配置见说明。

# 项目编译说明

## 第一步:编译整个项目
    mvn clean install
## 第二步:单独编译

>     进入retail-canaldata-consumer目录执行maven命令:mvn -Prelease clean install
>     之后会在整个项目主目录的target中看到retail-mysql-canal.monitor-1.0.tar.gz文件，解压该文件。配置后可在/bin目录中执行相应脚本可运行服务
>     此应用需要canal-server及kafka集群环境


# 配置文件说明

## kafka.properties(暂时未使用，可不关心)
## listener.properties(canal-server服务器连接配置)

    #canal注册到zk的集群地址
    canalServer.zklist=10.13.3.10:2181,10.13.3.12:2181
    #要采集的destination,此destination在server端需配置
    canalServer.destination=customer
    #采集指定的库列表
    canalServer.listenerdb=
    #采集指定的表列表
    canalServer.listenertable=
    #采集指定的事件列表(INSERT,UPDATE,DELETE),没有select
    canalServer.event=
    #暂时未使用
    canalServer.username=
    canalServer.password=


## spring/spring-config-kafka-producer.xml
    参看例子中即可


# 重要说明
>     此项目用到kafka，注意运行retail-mysql-canal.monitor-1.0的服务器要在本机hosts中配置kafka,broker host name的IP指向。否则会报kafka.common.FailedToSendMessageException: Failed to send messages after 10 tries异常