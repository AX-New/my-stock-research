# 数据如何落地存入到MongoDB数据库？

除了将数据存入关系型数据库（比如MySQL）外，也可以将数据存入MongoDB，以下是具体过程。

有如下几个步骤：

- 安装依赖包 pymongo

- 安装MongoDB（Linux） 1、下载安装包 curl -O https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-3.2.12.tgz 2、解压 tar -zxvf mongodb-linux-x86_64-3.2.12.tgz 3、移动到指定位置 mv mongodb-linux-x86_64-3.2.12/ /usr/local/mongodb 4、在/usr/local/mongodb下创建文件夹 mkdir -p /data/db mkdir /logs 5、在/usr/local/mongodb/bin下新建配置 vi mongodb.conf dbpath = /data/db logpath = /data/logs/mongodb.log port = 27017 fork = true nohttpinterface = true auth=true bind_ip=0.0.0.0 6、环境变量配置 vi /etc/profile export MONGODB_HOME=/usr/local/mongodb export PATH=$PATH:$MONGODB_HOME/bin 保存后，重启系统配置 source /etc/profile 保存后，重启系统配置 source /etc/profile 7、启动 在/usr/local/mongodb/bin下 mongod -f mongodb.conf 或 ./mongod -f mongodb.conf 8、关闭 mongod -f ./mongodb.conf --shutdown 或./mongod -f ./mongodb.conf --shutdown

- 编写入库代码 首先需要定义mongo的连接，这里给一个样例，大家可以自行修改。 import pandas as pd import tushare as ts from pymongo import MongoClient client = MongoClient(host='localhost', port=27017, username='root', password='mima123', authSource='admin', authMechanism='SCRAM-SHA-1') #存入数据 def insert_mongo(df): db = client['demos'] collection = db['stock_basic'] #print(df) collection.insert_many(df.to_dict('records'))

完整的入库程序，我们提供了一个完整的py文件供大家参考。可以通过关注Tushare官方公众号“挖地兔”，发送“mongo”获取代码下载链接：

![](https://tushare.pro/files/_images/ts.jpg)
