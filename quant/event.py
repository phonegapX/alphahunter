# -*— coding:utf-8 -*-

"""
Event Center.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import json
import zlib
import asyncio

import aioamqp

from quant import const
from quant.utils import logger
from quant.config import config
from quant.tasks import LoopRunTask, SingleTask
from quant.utils.decorator import async_method_locker
from quant.market import Orderbook, Trade, Kline, Ticker
from quant.asset import Asset
from quant.order import Order
from quant.position import Position


__all__ = ("EventCenter", "EventConfig", "EventHeartbeat", "EventAsset", "EventOrder", "EventKline", "EventOrderbook", "EventTrade", "EventTicker")


class Event:
    """ Event base.

    Attributes:
        name: Event name.
        exchange: Exchange name.
        queue: Queue name.
        routing_key: Routing key name.
        pre_fetch_count: How may message per fetched, default is 1.
        data: Message content.
    """

    def __init__(self, name=None, exchange=None, queue=None, routing_key=None, pre_fetch_count=1, data=None):
        """Initialize."""
        self._name = name
        self._exchange = exchange
        self._queue = queue
        self._routing_key = routing_key
        self._pre_fetch_count = pre_fetch_count
        self._data = data
        self._callback = None  # Asynchronous callback function.

    @property
    def name(self):
        return self._name

    @property
    def exchange(self):
        return self._exchange

    @property
    def queue(self):
        return self._queue

    @property
    def routing_key(self):
        return self._routing_key

    @property
    def prefetch_count(self):
        return self._pre_fetch_count

    @property
    def data(self):
        return self._data

    def dumps(self):
        d = {
            "n": self.name,
            "d": self.data
        }
        s = json.dumps(d)
        b = zlib.compress(s.encode("utf8"))
        return b

    def loads(self, b):
        b = zlib.decompress(b)
        d = json.loads(b.decode("utf8"))
        self._name = d.get("n")
        self._data = d.get("d")
        return d

    def parse(self):
        raise NotImplemented

    def subscribe(self, callback, multi=False):
        """ Subscribe this event.

        Args:
            callback: Asynchronous callback function.
            multi: If subscribe multiple channels ?
        """
        from quant.quant import quant
        self._callback = callback
        SingleTask.run(quant.event_center.subscribe, self, self.callback, multi)

    def publish(self):
        """Publish this event."""
        from quant.quant import quant
        SingleTask.run(quant.event_center.publish, self)

    async def callback(self, channel, body, envelope, properties):
        self._exchange = envelope.exchange_name
        self._routing_key = envelope.routing_key
        self.loads(body)
        o = self.parse()
        await self._callback(o)

    def __str__(self):
        info = "EVENT: name={n}, exchange={e}, queue={q}, routing_key={r}, data={d}".format(e=self.exchange, q=self.queue, r=self.routing_key, n=self.name, d=self.data)
        return info

    def __repr__(self):
        return str(self)


class EventConfig(Event):
    """ Config event.

    Attributes:
        server_id: Server id.
        params: Config params.

    * NOTE:
        Publisher: Manager Server.
        Subscriber: Any Servers who need.
    """

    def __init__(self, server_id=None, params=None):
        """Initialize."""
        name = "EVENT_CONFIG"
        exchange = "Config"
        queue = "{server_id}.{exchange}".format(server_id=server_id, exchange=exchange)
        routing_key = "{server_id}".format(server_id=server_id)
        data = {
            "server_id": server_id,
            "params": params
        }
        super(EventConfig, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        return self._data


class EventHeartbeat(Event):
    """ Server Heartbeat event.

    Attributes:
        server_id: Server id.
        count: Server heartbeat count.

    * NOTE:
        Publisher: All servers
        Subscriber: Monitor server.
    """

    def __init__(self, server_id=None, count=None):
        """Initialize."""
        name = "EVENT_HEARTBEAT"
        exchange = "Heartbeat"
        queue = "{server_id}.{exchange}".format(server_id=server_id, exchange=exchange)
        routing_key = "{server_id}".format(server_id=server_id)
        data = {
            "server_id": server_id,
            "count": count
        }
        super(EventHeartbeat, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        return self._data


class EventAsset(Event):
    """ Asset event.

    Attributes:
        platform: Exchange platform name, e.g. bitmex.
        account: Trading account name, e.g. test@gmail.com.
        assets: Asset details.
        timestamp: Publish time, millisecond.
        update: If any update in this publish.

    * NOTE:
        Publisher: Asset server.
        Subscriber: Any servers.
    """

    def __init__(self, platform=None, account=None, assets=None, timestamp=None, update=False):
        """Initialize."""
        name = "EVENT_ASSET"
        exchange = "Asset"
        routing_key = "{platform}.{account}".format(platform=platform, account=account)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "account": account,
            "assets": assets,
            "timestamp": timestamp,
            "update": update
        }
        super(EventAsset, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        asset = Asset(**self.data)
        return asset


class EventOrder(Event):
    """ Order event.

    Attributes:
        platform: Exchange platform name, e.g. binance/bitmex.
        account: Trading account name, e.g. test@gmail.com.
        strategy: Strategy name, e.g. my_test_strategy.
        order_no: order id.
        symbol: Trading pair name, e.g. ETH/BTC.
        action: Trading side, BUY/SELL.
        price: Order price.
        quantity: Order quantity.
        remain: Remain quantity that not filled.
        status: Order status.
        avg_price: Average price that filled.
        order_type: Order type, only for future order.
        ctime: Order create time, millisecond.
        utime: Order update time, millisecond.

    * NOTE:
        Publisher: Strategy Server.
        Subscriber: Any Servers who need.
    """

    def __init__(self, platform=None, account=None, strategy=None, order_no=None, symbol=None, action=None, price=None,
                 quantity=None, remain=None, status=None, avg_price=None, order_type=None, trade_type=None, ctime=None,
                 utime=None):
        """Initialize."""
        name = "EVENT_ORDER"
        exchange = "Order"
        routing_key = "{platform}.{account}.{strategy}".format(platform=platform, account=account, strategy=strategy)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "account": account,
            "strategy": strategy,
            "order_no": order_no,
            "action": action,
            "order_type": order_type,
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "remain": remain,
            "status": status,
            "avg_price": avg_price,
            "trade_type": trade_type,
            "ctime": ctime,
            "utime": utime
        }
        super(EventOrder, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        """ Parse self._data to Order object.
        """
        order = Order(**self.data)
        return order


class EventKline(Event):
    """ Kline event.

    Attributes:
        platform: Exchange platform name, e.g. bitmex.
        symbol: Trading pair, e.g. BTC/USD.
        open: Open price.
        high: Highest price.
        low: Lowest price.
        close: Close price.
        volume: Trade volume.
        timestamp: Publish time, millisecond.
        kline_type: Kline type, kline/kline_5min/kline_15min.

    * NOTE:
        Publisher: Market server.
        Subscriber: Any servers.
    """

    def __init__(self, platform=None, symbol=None, open=None, high=None, low=None, close=None, volume=None,
                 timestamp=None, kline_type=None, **kwargs):
        """Initialize."""
        if kline_type == const.MARKET_TYPE_KLINE:
            name = "EVENT_KLINE"
            exchange = "Kline"
        elif kline_type == const.MARKET_TYPE_KLINE_5M:
            name = "EVENT_KLINE_5MIN"
            exchange = "Kline.5min"
        elif kline_type == const.MARKET_TYPE_KLINE_15M:
            name = "EVENT_KLINE_15MIN"
            exchange = "Kline.15min"
        else:
            logger.error("kline_type error! kline_type:", kline_type, caller=self)
            return
        routing_key = "{platform}.{symbol}".format(platform=platform, symbol=symbol)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "symbol": symbol,
            "open": open,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "timestamp": timestamp,
            "kline_type": kline_type
        }
        data.update(kwargs) #如果是自合成K线的话就填充剩余字段
        super(EventKline, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        kline = Kline(**self.data)
        return kline


class EventOrderbook(Event):
    """ Orderbook event.

    Attributes:
        platform: Exchange platform name, e.g. bitmex.
        symbol: Trading pair, e.g. BTC/USD.
        asks: Asks, e.g. [[price, quantity], ... ]
        bids: Bids, e.g. [[price, quantity], ... ]
        timestamp: Publish time, millisecond.

    * NOTE:
        Publisher: Market server.
        Subscriber: Any servers.
    """

    def __init__(self, platform=None, symbol=None, asks=None, bids=None, timestamp=None):
        """Initialize."""
        name = "EVENT_ORDERBOOK"
        exchange = "Orderbook"
        routing_key = "{platform}.{symbol}".format(platform=platform, symbol=symbol)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "symbol": symbol,
            "asks": asks,
            "bids": bids,
            "timestamp": timestamp
        }
        super(EventOrderbook, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        orderbook = Orderbook(**self.data)
        return orderbook


class EventTrade(Event):
    """ Trade event.

    Attributes:
        platform: Exchange platform name, e.g. bitmex.
        symbol: Trading pair, e.g. BTC/USD.
        action: Trading side, BUY or SELL.
        price: Trade price.
        quantity: Trade size.
        timestamp: Publish time, millisecond.

    * NOTE:
        Publisher: Market server.
        Subscriber: Any servers.
    """

    def __init__(self, platform=None, symbol=None, action=None, price=None, quantity=None, timestamp=None):
        """ 初始化
        """
        name = "EVENT_TRADE"
        exchange = "Trade"
        routing_key = "{platform}.{symbol}".format(platform=platform, symbol=symbol)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "symbol": symbol,
            "action": action,
            "price": price,
            "quantity": quantity,
            "timestamp": timestamp
        }
        super(EventTrade, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        trade = Trade(**self.data)
        return trade


class EventTicker(Event):
    """ Ticker event.

    Attributes:
        platform: Exchange platform name, e.g. binance/bitmex.
        symbol: Trade pair name, e.g. ETH/BTC.
        ask: 市场当前最优卖价
        bid: 市场当前最优买价
        last:市场最新成交价
        timestamp: Update time, millisecond.

    * NOTE:
        Publisher: Market server.
        Subscriber: Any servers.
    """

    def __init__(self, platform=None, symbol=None, ask=None, bid=None, last=None, timestamp=None):
        """ 初始化
        """
        name = "EVENT_TICKER"
        exchange = "Ticker"
        routing_key = "{platform}.{symbol}".format(platform=platform, symbol=symbol)
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=config.server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        data = {
            "platform": platform,
            "symbol": symbol,
            "ask": ask,
            "bid": bid,
            "last": last,
            "timestamp": timestamp
        }
        super(EventTicker, self).__init__(name, exchange, queue, routing_key, data=data)

    def parse(self):
        ticker = Ticker(**self.data)
        return ticker


class EventCenter:
    """ Event center.
    """

    def __init__(self):
        self._host = config.rabbitmq.get("host", "localhost")
        self._port = config.rabbitmq.get("port", 5672)
        self._username = config.rabbitmq.get("username", "guest")
        self._password = config.rabbitmq.get("password", "guest")
        self._protocol = None
        self._channel = None  # Connection channel.
        self._connected = False  # If connect success.
        self._subscribers = []  # e.g. [(event, callback, multi), ...]
        self._event_handler = {}  # e.g. {"exchange:routing_key": [callback_function, ...]}

        # Register a loop run task to check TCP connection's healthy.
        LoopRunTask.register(self._check_connection, 10)

    def initialize(self):
        asyncio.get_event_loop().run_until_complete(self.connect())

    @async_method_locker("EventCenter.subscribe")
    async def subscribe(self, event: Event, callback=None, multi=False):
        """ Subscribe a event.

        Args:
            event: Event type.
            callback: Asynchronous callback.
            multi: If subscribe multiple channel(routing_key) ?
        """
        logger.info("NAME:", event.name, "EXCHANGE:", event.exchange, "QUEUE:", event.queue, "ROUTING_KEY:", event.routing_key, caller=self)
        self._subscribers.append((event, callback, multi))

    async def publish(self, event):
        """ Publish a event.

        Args:
            event: A event to publish.
        """
        if not self._connected:
            logger.warn("RabbitMQ not ready right now!", caller=self)
            return
        data = event.dumps()
        try:
            await self._channel.basic_publish(payload=data, exchange_name=event.exchange, routing_key=event.routing_key)
        except Exception as e:
            logger.error("publish error:", e, caller=self)

    async def connect(self, reconnect=False):
        """ Connect to RabbitMQ server and create default exchange.

        Args:
            reconnect: If this invoke is a re-connection ?
        """
        logger.info("host:", self._host, "port:", self._port, caller=self)
        if self._connected:
            return

        # Create a connection.
        try:
            transport, protocol = await aioamqp.connect(host=self._host, port=self._port, login=self._username, password=self._password, login_method="PLAIN")
        except Exception as e:
            logger.error("connection error:", e, caller=self)
            return
        finally:
            if self._connected:
                return
        channel = await protocol.channel()
        self._protocol = protocol
        self._channel = channel
        self._connected = True
        logger.info("Rabbitmq initialize success!", caller=self)

        # Create default exchanges.
        exchanges = ["Orderbook", "Trade", "Kline", "Kline.5min", "Kline.15min", "Config", "Heartbeat", "Asset", "Order", "Ticker"]
        for name in exchanges:
            await self._channel.exchange_declare(exchange_name=name, type_name="topic")
        logger.debug("create default exchanges success!", caller=self)

        if reconnect:
            self._bind_and_consume()
        else:
            # Maybe we should waiting for all modules to be initialized successfully.
            asyncio.get_event_loop().call_later(5, self._bind_and_consume)

    def _bind_and_consume(self):
        async def _call_async():
            for event, callback, multi in self._subscribers:
                await self._do_subscribe(event, callback, multi)
        SingleTask.run(_call_async)

    async def _do_subscribe(self, event: Event, callback=None, multi=False):
        if event.queue:
            await self._channel.queue_declare(queue_name=event.queue, auto_delete=True)
            queue_name = event.queue
        else:
            result = await self._channel.queue_declare(exclusive=True)
            queue_name = result["queue"]
        await self._channel.queue_bind(queue_name=queue_name, exchange_name=event.exchange, routing_key=event.routing_key)
        await self._channel.basic_qos(prefetch_count=event.prefetch_count)
        if callback:
            if multi:
                await self._channel.basic_consume(callback=callback, queue_name=queue_name, no_ack=True)
                logger.info("multi message queue:", queue_name, caller=self)
            else:
                await self._channel.basic_consume(self._on_consume_event_msg, queue_name=queue_name)
                logger.info("queue:", queue_name, caller=self)
                self._add_event_handler(event, callback)

    async def _on_consume_event_msg(self, channel, body, envelope, properties):
        #logger.debug("exchange:", envelope.exchange_name, "routing_key:", envelope.routing_key, "body:", body, caller=self)
        try:
            key = "{exchange}:{routing_key}".format(exchange=envelope.exchange_name, routing_key=envelope.routing_key)
            funcs = self._event_handler[key]
            for func in funcs:
                SingleTask.run(func, channel, body, envelope, properties)
        except:
            logger.error("event handle error! body:", body, caller=self)
            return
        finally:
            await self._channel.basic_client_ack(delivery_tag=envelope.delivery_tag)  # response ack

    def _add_event_handler(self, event: Event, callback):
        key = "{exchange}:{routing_key}".format(exchange=event.exchange, routing_key=event.routing_key)
        if key in self._event_handler:
            self._event_handler[key].append(callback)
        else:
            self._event_handler[key] = [callback]
        logger.debug("event handlers:", self._event_handler.keys(), caller=self)

    async def _check_connection(self, *args, **kwargs):
        if self._connected and self._channel and self._channel.is_open:
            logger.debug("RabbitMQ connection ok.", caller=self)
            return
        logger.error("CONNECTION LOSE! START RECONNECT RIGHT NOW!", caller=self)
        self._connected = False
        self._protocol = None
        self._channel = None
        self._event_handler = {}
        SingleTask.run(self.connect, reconnect=True)
