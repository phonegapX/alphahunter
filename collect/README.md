
## 交易所市场行情采集服务

本目录下包含了各个交易所行情采集服务。

### 1. 基本说明

行情采集服务从交易所获取行情数据,写入数据库(可选),并且发布到RabbitMQ事件中心。采集的数据包括k线数据,市场成交数据,盘口数据等。  
每个目录代表相应交易所的行情采集服务


### 2. 配置文件

详见config.json:
- RABBITMQ 指定事件中心服务器；
- PROXY HTTP代理，翻墙，你懂的；（如果在不需要翻墙的环境运行，此参数可以去掉）
- PLATFORMS 行情采集的目标交易所；
- MARKETS 行情采集的目标交易对(合约)；
- strategy 服务名称；

配置文件比较简单，更多的配置可以参考 [配置文件说明](../docs/configure/README.md)。


### 3. 启动程序

我们介绍了`alphahunter` 行情采集服务,现在让我们来启动程序,进入某个服务目录,如collect/ftx,然后运行:
```text
python ./main.py
```


### 4. 参考文档

- [config 服务配置](../docs/configure/README.md)
- [EventCenter 安装RabbitMQ](../docs/others/rabbitmq_deploy.md)
- [Logger 日志打印](../docs/others/logger.md)
- [locker 并发锁](../docs/others/locker.md)
- [Python Asyncio](https://docs.python.org/3/library/asyncio.html)
