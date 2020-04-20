用于学习 kafka 的 demo

[swagger-ui.html](http://localhost:8787/demo_kafka/swagger-ui.html)

[index.html](http://localhost:8787/demo_kafka/index.html)



目标功能
1. cluster
   * 查看集群的brokers（概览）
   * 查看集群的controller（单个）
2. broker
   * 查看所有的brokers(list)
   * 查看每个broker的Topics整体信息(目前似乎只有topic数量)
3. topic
   * 查看broker所有的topics(list)
   * 删除topic
   * 新增topic
   * 查看每个topic的consumer
   * 查看每个topic的partition概览，数量概览
   * 针对每个topic的配置进行CRUD
   * topic数据的快速清空(就是删除再创建)
   * topic数据的复制（默认全数据，支持百分比，因为包含了多个Partition，使用百分比最好，数字默认转换为百分比）
     * 自身复制到自身
     * 复制到其他的topic（partition）
     * clone成另外一个topic
   * topic级别数据检索（多条件，默认全Partition和指定Partition）
     * 按key检索(支持正则)
     * 按value检索(支持正则)
     * 按offset检索
     * 按keySize检索
     * 按valueSize检索
     * 按timestamp检索(支持格化)  
     * 按timestampType检索(目前只有两个创建时间和生成时间类型)  
4. partition
   * 查看topic的所有partition(list)
   * 新增partition
   * 查看partition的consumer列表
   * partition数据复制
     * 自身复制到自身
     * 复制到其他的partition
   * partition级别数据检索(多条件)
     * 按key检索(支持正则)
     * 按value检索(支持正则)
     * 按offset检索
     * 按keySize检索
     * 按valueSize检索
     * 按timestamp检索(支持格化)  
     * 按timestampType检索(目前只有两个创建时间和生成时间类型)    
5. consumer
   * consumer的list
   * consumer的删除
   * 模拟consumer的消费(采用websocket模式)
   * 单个consumer的成员信息
   * 单个consumer的订阅topic的list
   * 单个consumer的订阅的每个topic的具体的offset
   * 单个consumer的订阅的每个topic的offset的seek
6. producer(注意字符串的转义，目标为转成BASE64)
   * 发送指定Topic的消息
   * 发送指定Partition的消息
   * 需要支持上传文件，按行上传
   * 需要支持上传多个文件，转码后上传





  

 

 