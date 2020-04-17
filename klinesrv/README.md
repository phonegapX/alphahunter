
## 自合成K线服务

本目录下包含了针对各个交易所的自合成K线服务。

### 1. 基本说明

自合成K线服务每分钟从数据库中读取逐笔成交数据,合成K线后写入数据库,并且发布到RabbitMQ事件中心。  
db_create_index目录是一个为数据库建立查询索引的工具,用于加快数据库查询速度。  
其他目录代表相应交易所的自合成K线服务。


### 2. 配置文件

详见config.json:
- RABBITMQ 指定事件中心服务器；
- PLATFORMS 自合成K线服务的目标交易所；
- MARKETS 自合成K线服务的目标交易对(合约)；
- strategy 服务名称；

配置文件比较简单，更多的配置可以参考 [配置文件说明](../docs/configure/README.md)。


### 3. 启动程序

我们介绍了`alphahunter` 自合成K线服务,现在让我们来启动程序,进入某个服务目录,如klinesrv/huobi,然后运行:
```text
python ./main.py
```


### 4. 参考文档

- [config 服务配置](../docs/configure/README.md)
- [EventCenter 安装RabbitMQ](../docs/others/rabbitmq_deploy.md)
- [Logger 日志打印](../docs/others/logger.md)
- [locker 并发锁](../docs/others/locker.md)
- [Python Asyncio](https://docs.python.org/3/library/asyncio.html)
