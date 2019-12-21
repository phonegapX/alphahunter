
## 策略例子演示

本目录下包含了连接各个交易所的策略示例代码。

### 1. 准备条件

在开发策略之前，需要先对整套系统的运行原理有一个大致的了解，以及相应的开发环境和运行环境。

- Python3.x开发环境，并安装好 `alphahunter` 开发包；
- 部署 [RabbitMQ 事件中心服务](../../docs/others/rabbitmq_deploy.md) ---- 事件中心的核心组成部分；
- 部署 [Market 行情服务](../collect/README.md)(可选) ---- 服务位于alphahunter/collect/xxx/main.py, 服务从交易所获取行情,并且发布到RabbitMQ 事件中心，如果策略不需要行情数据或者策略直接从交易所获取行情数据，那么此服务可以不用部署；
- 注册相应交易所账户，并且创建 `ACCESS KEY` 和 `SECRET KEY`，AK有操作委托单权限；


### 2. 策略示例讲解

演示代码如下:

```python
class DemoStrategy(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(DemoStrategy, self).__init__()

        self.strategy = config.strategy

        #=====================================================
        #创建一个交易接口
        self.platform = config.accounts[0]["platform"]
        self.account = config.accounts[0]["account"]
        self.access_key = config.accounts[0]["access_key"]
        self.secret_key = config.accounts[0]["secret_key"]
        target = config.markets[self.platform]
        self.symbols = target["symbols"]
        # 交易模块参数
        params = {
            "strategy": self.strategy, #策略名称
            "platform": self.platform, #交易平台
            "symbols": self.symbols,   #交易符号列表
            "account": self.account,   #apikey对应账号,请真实认真填写
            "access_key": self.access_key, #apikey
            "secret_key": self.secret_key, #apikey

            "enable_kline_update": True,     #是否启用k线通知回调(市场公共数据)
            "enable_orderbook_update": True, #是否启用盘口数据通知回调(市场公共数据)
            "enable_trade_update": True,     #是否启用市场最新成交通知回调(市场公共数据)
            "enable_ticker_update": True,    #是否启用tick数据通知回调(市场公共数据)
            "enable_order_update": True,     #是否启用用户挂单通知回调(用户账户私有数据)
            "enable_fill_update": True,      #是否启用挂单成交通知回调(用户账户私有数据)
            "enable_position_update": True,  #是否启用仓位更新通知回调(用户账户私有数据)
            "enable_asset_update": True,     #是否启用资产更新通知回调(用户账户私有数据)

            "direct_kline_update": False,     #直连交易所获取行情数据or从RABBITMQ事件中心订阅(需要启用相应行情采集服务)
            "direct_orderbook_update": False, #直连交易所获取行情数据or从RABBITMQ事件中心订阅(需要启用相应行情采集服务)
            "direct_trade_update": False,     #直连交易所获取行情数据or从RABBITMQ事件中心订阅(需要启用相应行情采集服务)
            "direct_ticker_update": False     #直连交易所获取行情数据or从RABBITMQ事件中心订阅(需要启用相应行情采集服务)
        }
        self.trader = self.create_gateway(**params)#创建交易所接口

        #当然你接下来还可以再创建另外一个交易所的接口,
        #比如你要做搬砖套利的策略，需要同时操作2个不同交易所，假如为self.trader1

        # 注册定时器
        self.enable_timer()  # 每隔1秒执行一次回调

    async def on_time(self):
        """ 每秒钟执行一次. 因为是异步并发架构,这个函数执行的时候交易通道链接不一定已经建立好
        """
        if not hasattr(self, "just_once"):
            self.just_once = 1
            #xx = self.get_orders(self.trader, "ETH-PERP")
            xx = self.get_position(self.trader1, "ETH-PERP")
            #xx = self.get_assets(self.trader)
            #xx = self.create_order(self.trader1, "ETH-PERP", ORDER_ACTION_SELL, "51", "-0.002")
            #xx = self.create_order(self.trader1, "ETH-PERP", ORDER_ACTION_SELL, "0", "-0.002", ORDER_TYPE_MARKET)
            #xx = self.revoke_order(self.trader, "ETH-PERP", "1017521392")
            #order1 = Strategy.TOrder(self.trader, "ETH-PERP", ORDER_ACTION_SELL, "351", "-0.02")
            #order2 = Strategy.TOrder(self.trader1, "ETH-PERP", ORDER_ACTION_SELL, "352", "-0.03")
            #xx = self.create_pair_order(order1, order2)
            #xx = self.get_symbol_info(self.trader, "ETH-PERP")
            yy, zz = await xx

        logger.info("on_time ...", caller=self)

    async def on_init_success_callback(self, success: bool, error: Error, **kwargs):
        """ 初始化状态通知
        """
        logger.info("on_init_success_callback:", success, caller=self)

    async def on_kline_update_callback(self, kline: Kline):
        """ 市场K线更新
        """
        logger.info("kline:", kline, caller=self)

    @async_method_locker("DemoStrategy.can_do_open_close_pos_demo.locker", False)
    async def can_do_open_close_pos_demo(self):
        """
        开平仓逻辑应该独立放到一个函数里面,并且加上'不等待类型的锁',就像本函数演示的这样.
        因为为了最大的时效性,框架采用的是异步架构,假如这里还在处理过程中,新的回调通知来了,那样就会
        引起重复开平仓,所以就把开平仓的过程加上'不等待类型的锁',这样新的回调通知来了,这里又被调用的情况下,
        因为有'不等待类型的锁',所以会直接跳过(忽略)本函数,这样就不会导致重复执行开平仓的动作.
        记住这里是'不等待类型的锁'(装饰器第二个参数为False),而不是`等待类型的锁`,因为我们不需要等待,假如等待的话还是会重复开平仓(而且行情也过期了)
        比如下面模拟要处理3秒,现实中是有可能发生的,比如网络或者交易所繁忙的时候.
        """
        await asyncio.sleep(3)

    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄更新
        """
        logger.info("orderbook:", orderbook, caller=self)
        #ask1_price = float(orderbook.asks[0][0])  # 卖一价格
        #bid1_price = float(orderbook.bids[0][0])  # 买一价格
        #self.current_price = (ask1_price + bid1_price) / 2  # 为了方便，这里假设盘口价格为 卖一 和 买一 的平均值
        """
        假设策略在本回调函数里面判断开平仓条件,并且条件达到可以进行开平仓的情况下,最好是把接下来的开平仓逻辑单独
        放在一个函数里面,并且加上'不等待类型的锁',比如下面这个函数这样.
        """
        #if 开平仓条件达到:
        await self.can_do_open_close_pos_demo()
        print("##################################")

    async def on_trade_update_callback(self, trade: Trade):
        """ 市场最新成交更新
        """
        logger.info("trade:", trade, caller=self)

    async def on_ticker_update_callback(self, ticker: Ticker):
        """ 市场行情tick更新
        """
        logger.info("ticker:", ticker, caller=self)

    async def on_order_update_callback(self, order: Order):
        """ 订单状态更新
        """
        logger.info("order:", order, caller=self)

    async def on_fill_update_callback(self, fill: Fill):
        """ 订单成交通知
        """
        logger.info("fill:", fill, caller=self)

    async def on_position_update_callback(self, position: Position):
        """ 持仓更新
        """
        logger.info("position:", position, caller=self)

    async def on_asset_update_callback(self, asset: Asset):
        """ 账户资产更新
        """
        logger.info("asset:", asset, caller=self)
```

首先你的策略类需要继承至`Strategy`类,并且实现`ExchangeGateway.ICallBack`接口：

`async def on_init_success_callback(self, success: bool, error: Error, **kwargs)` 初始化状态通知

`async def on_kline_update_callback(self, kline: Kline)` 市场K线更新(公共数据)

`async def on_orderbook_update_callback(self, orderbook: Orderbook)` 订单薄更新(公共数据)

`async def on_trade_update_callback(self, trade: Trade)` 市场最新成交更新(公共数据)

`async def on_ticker_update_callback(self, ticker: Ticker)` 市场行情tick更新(公共数据)

`async def on_order_update_callback(self, order: Order)` 订单状态更新(私有数据)

`async def on_fill_update_callback(self, fill: Fill)` 订单成交通知(私有数据)

`async def on_position_update_callback(self, position: Position)` 持仓更新(私有数据)

`async def on_asset_update_callback(self, asset: Asset)` 账户资产更新(私有数据)

你可以使用`enable_timer`使能定时器:`async def on_time(self)`，默认一秒执行一次

接下来介绍策略相关API:

`get_orders` 获取当前订单列表

`get_position` 获取当前仓位

`get_assets` 获取当前资产

`create_order` 创建订单

`revoke_order` 撤销订单

`create_pair_order` 同时创建一对订单，用于比如配对交易，统计套利等需要同时操作2个symbol的场景

`get_symbol_info` 获取指定symbol的一些信息,比如最小下单量等

`create_gateway` 创建交易接口

`pm` 属性字段,返回一个与这个策略有关的数据管理器`PortfolioManager`,统一缓存并管理这个策略的仓位,订单,资产,成交等信息

重要参数:
```python
    # 交易模块参数
    params = {
        "strategy": self.strategy, #策略名称
        "platform": self.platform, #交易平台
        "symbols": self.symbols,   #交易符号列表
        "account": self.account,   #apikey对应账号,请真实认真填写
        "access_key": self.access_key, #apikey
        "secret_key": self.secret_key, #apikey
    
        "enable_kline_update": True,     #是否启用k线通知回调(市场公共数据)
        "enable_orderbook_update": True, #是否启用盘口数据通知回调(市场公共数据)
        "enable_trade_update": True,     #是否启用市场最新成交通知回调(市场公共数据)
        "enable_ticker_update": True,    #是否启用tick数据通知回调(市场公共数据)
        "enable_order_update": True,     #是否启用用户挂单通知回调(用户账户私有数据)
        "enable_fill_update": True,      #是否启用挂单成交通知回调(用户账户私有数据)
        "enable_position_update": True,  #是否启用仓位更新通知回调(用户账户私有数据)
        "enable_asset_update": True,     #是否启用资产更新通知回调(用户账户私有数据)
    
        "direct_kline_update": False,     #直连交易所获取行情数据or从RABBITMQ事件中心订阅(需要启用相应行情采集服务)
        "direct_orderbook_update": False, #直连交易所获取行情数据or从RABBITMQ事件中心订阅(需要启用相应行情采集服务)
        "direct_trade_update": False,     #直连交易所获取行情数据or从RABBITMQ事件中心订阅(需要启用相应行情采集服务)
        "direct_ticker_update": False     #直连交易所获取行情数据or从RABBITMQ事件中心订阅(需要启用相应行情采集服务)
    }
    self.trader = self.create_gateway(**params)#创建交易所接口
```


### 3. 程序入口

我们的策略逻辑已经完成，现在我们需要初始化 `alphahunter` 框架，并加载我们的策略，让底层框架驱动策略运行起来。

```python
def main():
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = None

    from quant.quant import quant
    quant.initialize(config_file)
    DemoStrategy()
    quant.start()


if __name__ == '__main__':
    main()
```

> 我们首先判断程序运行的第一个参数是否指定了配置文件，配置文件一般为 `config.json` 的json文件，如果没有指定配置文件，那么就设置配置文件为None。
其次，我们导入 `quant` 模块，调用 `quant.initialize(config_file)` 初始化配置，紧接着执行 `DemoStrategy()` 初始化策略，最后执行 `quant.start()` 启动整个程序。


##### 3.1 配置文件

详见config.json:
- RABBITMQ 指定事件中心服务器；
- PROXY HTTP代理，翻墙，你懂的；（如果在不需要翻墙的环境运行，此参数可以去掉）
- ACCOUNTS 指定需要使用的交易账户，注意account用于标识账户,一定要真实认真填写；
- MARKETS 策略运行的交易对(合约)
- strategy 策略的名称；

配置文件比较简单，更多的配置可以参考 [配置文件说明](../docs/configure/README.md)。


### 4. 启动程序

以上，我们介绍了如何使用 `alphahunter` 开发自己的策略，现在让我们来启动程序,进入某个例子目录，如example/ftx，然后运行:
```text
python ./main.py config.json
```

### 5. 参考文档

- [config 服务配置](../docs/configure/README.md)
- [EventCenter 安装RabbitMQ](../docs/others/rabbitmq_deploy.md)
- [Logger 日志打印](../docs/others/logger.md)
- [locker 并发锁](../docs/others/locker.md)
- [Collect 行情采集服务](../collect/README.md)
- [Python Asyncio](https://docs.python.org/3/library/asyncio.html)
