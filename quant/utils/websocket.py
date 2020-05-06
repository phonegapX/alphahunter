# -*— coding:utf-8 -*-

"""
websocket接口封装

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import json
import aiohttp
import asyncio

from quant.utils import logger, tools
from quant.config import config
from quant.heartbeat import heartbeat
from quant.state import State
from quant.tasks import SingleTask


class Websocket:
    """ websocket接口封装
    """

    def __init__(self, url, check_conn_interval=60, send_hb_interval=10, **kwargs):
        """ 初始化
        @param url 建立websocket的地址
        @param check_conn_interval 检查websocket连接时间间隔
        @param send_hb_interval 发送心跳时间间隔，如果是0就不发送心跳消息
        """
        self.cb = kwargs["cb"]
        self._platform = kwargs.get("platform")
        self._account = kwargs.get("account")
        self._url = url
        self._check_conn_interval = check_conn_interval
        if self._check_conn_interval < 15:
            self._check_conn_interval = 15 #值不能小于15秒
        self._send_hb_interval = send_hb_interval
        self._ws = None #websocket连接对象
        self.heartbeat_msg = None #心跳消息
        self.last_timestamp = 0 #最后一次收到消息的时间(包括pingpong包).[对于不支持pingpong机制的交易所这个值必须强制为0,下一步再添加相关代码]
        self.session = None

    @property
    def ws(self):
        return self._ws
    
    def initialize(self):
        """ 初始化
        """
        #注册服务 检查连接是否正常
        heartbeat.register(self._check_connection, 5) #5秒检测一次
        #注册服务 发送心跳
        if self._send_hb_interval > 0:
            heartbeat.register(self._send_heartbeat_msg, self._send_hb_interval)
        #建立websocket连接
        asyncio.get_event_loop().create_task(self._connect())

    async def _connect(self):
        logger.info("url:", self._url, caller=self)
        proxy = config.proxy
        if not self.session:
            self.session = aiohttp.ClientSession()
        try:
            self._ws = await self.session.ws_connect(self._url, proxy=proxy)
        except (aiohttp.client_exceptions.ClientConnectorError, 
                aiohttp.client_exceptions.ClientHttpProxyError, 
                aiohttp.client_exceptions.ServerDisconnectedError, 
                aiohttp.client_exceptions.WSServerHandshakeError, 
                asyncio.TimeoutError) as e:
            logger.error("connect to server error! url:", self._url, caller=self)
            state = State(self._platform, self._account, "connect to server error! url: {}, error: {}".format(self._url, e), State.STATE_CODE_CONNECT_FAILED)
            SingleTask.run(self.cb.on_state_update_callback, state)
            asyncio.get_event_loop().create_task(self._reconnect()) #如果连接出错就重新连接
            return
        state = State(self._platform, self._account, "connect to server success! url: {}".format(self._url), State.STATE_CODE_CONNECT_SUCCESS)
        SingleTask.run(self.cb.on_state_update_callback, state)
        asyncio.get_event_loop().create_task(self.connected_callback())
        asyncio.get_event_loop().create_task(self.receive())

    async def _reconnect(self, delay=5):
        """ 重新建立websocket连接
        """
        if delay > 0:
            await asyncio.sleep(delay) #等待一段时间再重连
        logger.warn("reconnecting websocket right now!", caller=self)
        state = State(self._platform, self._account, "reconnecting websocket right now! url: {}".format(self._url), State.STATE_CODE_RECONNECTING)
        SingleTask.run(self.cb.on_state_update_callback, state)
        await self._connect()

    async def connected_callback(self):
        """ 连接建立成功的回调函数
        * NOTE: 子类继承实现
        """
        pass

    async def receive(self):
        """ 接收消息
        """
        """
        See: client_ws.py
        
        async def __anext__(self):
            msg = await self.receive()
            if msg.type in (WSMsgType.CLOSE,
                            WSMsgType.CLOSING,
                            WSMsgType.CLOSED):
                raise StopAsyncIteration  # NOQA
            return msg
        """
        self.last_timestamp = tools.get_cur_timestamp() #单位:秒,连接检测时间初始化
        async for msg in self.ws: #参考aiohttp的源码,当ws连接被关闭后,本循环将退出
            self.last_timestamp = tools.get_cur_timestamp() #单位:秒,每收到一个消息就更新一下此变量,用于判断网络是否出问题,是否需要重连
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except:
                    data = msg.data
                #await asyncio.get_event_loop().create_task(self.process(data)) #这样写的好处是如果里面这个函数发生异常不会影响本循环,因为它在单独任务里面执行
                try:
                    await self.process(data)
                except Exception as e:
                    logger.error("process ERROR:", e, caller=self)
                    await self.socket_close() #关闭
                    break #退出循环
            elif msg.type == aiohttp.WSMsgType.BINARY:
                #await asyncio.get_event_loop().create_task(self.process_binary(msg.data)) #好处同上
                try:
                    await self.process_binary(msg.data)
                except Exception as e:
                    logger.error("process_binary ERROR:", e, caller=self)
                    await self.socket_close() #关闭
                    break #退出循环
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error("receive event ERROR:", msg, caller=self)
                break #退出循环
            else:
                #aiohttp.WSMsgType.CONTINUATION
                #aiohttp.WSMsgType.PING
                #aiohttp.WSMsgType.PONG
                logger.warn("unhandled msg:", msg, caller=self)
        #当代码执行到这里的时候ws已经是关闭状态,所以不需要再调用close去关闭ws了.
        #当ws连接被关闭或者出现任何错误,将重新连接
        state = State(self._platform, self._account, "connection lost! url: {}".format(self._url), State.STATE_CODE_DISCONNECT)
        SingleTask.run(self.cb.on_state_update_callback, state)
        self.last_timestamp = 0 #先置0,相当于关闭连接检测
        asyncio.get_event_loop().create_task(self._reconnect())

    async def process(self, msg):
        """ 处理websocket上接收到的消息 text 类型
        * NOTE: 子类继承实现
        """
        raise NotImplementedError

    async def process_binary(self, msg):
        """ 处理websocket上接收到的消息 binary类型
        * NOTE: 子类继承实现
        """
        raise NotImplementedError

    async def _check_connection(self, *args, **kwargs):
        """ 检查连接是否正常
        """
        now = tools.get_cur_timestamp() #当前时间,单位:秒
        if self.last_timestamp > 0 and now - self.last_timestamp > self._check_conn_interval: #最后接收数据包(包括pingpong包)时间间隔超过指定值
            if self.ws and not self.ws.closed: #连接没有关闭
                await self.socket_close() #关闭

    async def _send_heartbeat_msg(self, *args, **kwargs):
        """ 发送心跳给服务器
        """
        if not self.ws:
            logger.warn("websocket connection not connected yet!", caller=self)
            return
        if self.heartbeat_msg:
            if isinstance(self.heartbeat_msg, dict):
                await self.send_json(self.heartbeat_msg)
            elif isinstance(self.heartbeat_msg, str):
                await self.send_str(self.heartbeat_msg)
            else:
                logger.error("send heartbeat msg failed! heartbeat msg:", self.heartbeat_msg, caller=self)
                return
            logger.debug("send ping message:", self.heartbeat_msg, caller=self)

    async def send_json(self, data):
        if self.ws != None:
            """
            aiohttp文档中的说明:
            
            RuntimeError – if connection is not started or closing
            ValueError – if data is not serializable object
            TypeError – if value returned by dumps(data) is not str
            """
            try:
                await self.ws.send_json(data)
            except RuntimeError:
                await self.socket_close() #网络原因引起的错误,关闭当前连接
            except ValueError:
                pass #不是网络原因引起的错误,直接忽略
            except TypeError:
                pass #不是网络原因引起的错误,直接忽略
            except Exception:
                await self.socket_close() #其他任何类型的未知错误,关闭当前连接

    async def send_str(self, data):
        if self.ws != None:
            try:
                await self.ws.send_str(data)
            except RuntimeError:
                await self.socket_close() #网络原因引起的错误,关闭当前连接
            except ValueError:
                pass #不是网络原因引起的错误,直接忽略
            except TypeError:
                pass #不是网络原因引起的错误,直接忽略
            except Exception:
                await self.socket_close() #其他任何类型的未知错误,关闭当前连接

    async def socket_close(self):
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass