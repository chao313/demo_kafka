用于学习 kafka 的 demo

[swagger-ui.html](http://localhost:8787/demo_kafka/swagger-ui.html)

[index.html](http://localhost:8787/demo_kafka/index.html)


注意:
* 缺少配置的CRUD及解释
* 缺少流式处理
* 缺少命令行
* 缺少监控指标
* 缺少connect数据ETL(Mysql->ES)

目标功能
1. cluster
   * [x] 查看集群的brokers（概览）
   * [x] 查看集群的controller（单个）
2. broker
   * [x] 查看所有的brokers(list)
   * [x] 查看每个broker的Topics整体信息(topic数量)
   * [x] 查看每个broker的consumer整体信息(consumer数量)
3. topic
   * [x] 查看broker所有的topics(list)
   * [x] 删除topic
   * [x] 新增topic
   * [x] 查看每个topic的consumer
   * [x] 查看每个topic的partition概览，数量概览
   * [x] 查看每个topic有效offset
   * [x] 查看每个topic总共offset
   * 针对每个topic的配置进行CRUD
   * [x] topic数据的快速清空(就是删除再创建)
   * topic数据的复制（默认全数据，支持百分比，因为包含了多个Partition，使用百分比最好，数字默认转换为百分比）
     * 自身复制到自身
     * 复制到其他的topic（partition）
     * clone成另外一个topic
   * topic级别数据检索（多条件，默认全Partition和指定Partition）
     * [x] 按key检索(支持正则)
     * [x] 按value检索(支持正则)
     * [x] 按timestamp检索(支持格化)  
     * 按timestampType检索(目前只有两个创建时间和生成时间类型)  
4. partition
   * [x] 查看topic的所有partition(list)
   *  新增partition
   * partition数据复制
     * 自身复制到自身
     * 复制到其他的partition
   * [x] partition级别数据检索(多条件)
     * [x] 按key检索(支持正则)
     * [x] 按value检索(支持正则)
     * [x] 按offset检索
     * [x] 按timestamp检索(支持格化)  
     * 按timestampType检索(目前只有两个创建时间和生成时间类型)    
5. consumer
   * [x] consumer的list
   * [x] consumer的删除
   * 模拟consumer的消费(采用websocket模式)
   * [x] 单个consumer的成员信息
   * [x] 单个consumer的订阅topic的list
   * [x] 单个consumer的订阅的每个topic的具体的offset
   * [x] 单个consumer的订阅的每个topic的offset的seek
6. producer(注意字符串的转义，目标为转成BASE64)
   * [x] 发送指定Topic的消息
   * [x] 发送指定Partition的消息
   * [x] 需要支持上传文件，按行上传
   * [x] 需要支持上传多个文件，转码后上传
   
---

图片:
集群管理
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/集群管理.png)
配置查看
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/配置查看.png)
Topic列表
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/Topic列表.png)
topic级别的简单查看
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/topic级别的简单查看.png)
Consumer的偏移量挑拨
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/Consumer的偏移量挑拨.png)
Partition列表
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/Partition列表.png)
更新Patition数量
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/更新Patition数量.png)
Partition简单查看
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/Partition简单查看.png)
Partition的高级查看
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/Partition的高级查看.png)
Produce发送
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/Produce发送.png)
消费者列表
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/消费者列表.png)
关系图
![image](https://raw.githubusercontent.com/chao313/demo_kafka/master/src/main/resources/doc/kafka-admin-produce-consumer关系图.png)





   





  

 

 