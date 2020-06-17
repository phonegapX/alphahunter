# 量化交易接口API (纯websocket版)
version: 1.0.0

## 接入URL
> ws://127.0.0.1:9878/ws/v1



### 使用提示
当我们连接上服务端以后,需要首先发送登录请求,登录某个交易接口,这里需要注意的是当接收到登录应答以后并不代表登录已经成功,
登录请求仅仅只是发出一个登录意向,服务端后台需要为登录接口做大量的初始化工作,这个时候我们需要等待通知,当登录成功或者某些原因失败,服务端都会通过`状态通知消息(op=state)`通知我们,
比如当成功的情况下我们将收到code为STATE_CODE_READY的通知消息,当收到这个通知以后我们才能真正进行各种操作,比如下单等. 在正常运行中我们也应该时刻关注`状态通知消息(op=state)`因为运行过程中
交易接口出现任何问题服务端都会通过`状态通知消息(op=state)`通知我们,举例说明,比如说服务端因为网络原因与实际交易所连接断开,服务端会通过`状态通知消息(op=state)`通知我们, 
那么这个时候我们只需要耐心等待,因为服务端会自动重连交易所,当一切恢复正常后,服务端会再次发送code为STATE_CODE_READY的`状态通知消息(op=state)`通知我们,然后我们就可以继续进行各种操作了,比如下单. 

我们可以在一个websocket连接上同时登录多个交易接口,比如说可以同时登录火币现货接口和OKEX现货接口,那么我们怎么区分这两个接口呢?这就需要消息中的token字段,token是登录凭证,不同的token代表不同的交易接口.

量化交易API的各种操作都对时序要求不高,具有异步性质,可以一次性发出多个请求,比如说可以向火币现货接口下单后马上又向OKEX现货接口下单,而不需要先等待前一个操作的应答,再发出第二个请求.
而收到应答的顺序也不一定和发送请求的顺序是一一对应的,那么我们该如何判断哪个应答对应哪个请求呢?这就需要消息中的cid字段,cid代表客户自定义id,通过判断这个字段就可以将请求和应答一一对应起来.



### 心跳信息
服务端如果十五秒内没有收到任何类型的消息,服务端将会主动断开连接.客户端应该每五秒钟向服务端发送ping,服务端会返回pong.

>ping

```json
    {
        "op": "ping",
        "ts": 1530604762311
    }
```

>pong

```json
    {
        "op": "pong",
        "ts": 1530604762311
    }
```



### 1.登录
任何操作前都必须先登录

>请求示例
```json
    {
        "op": "login",
        "cid": "xxxxx",
        "platform": "huobi_future",
        "symbols": ["BTC/USDT","ETH/USDT"],
        "access_key": "xxxxxxxxxxxxx",
        "secret_key": "xxxxxxxxxxxxx"
    }
```

| Name       | Description            | Required | Schema       |
| ---------- | ---------------------- | -------- | ------------ |
| op         | 操作名称               | Yes      | string       |
| cid        | 请求自定义ID           | No       | string       |
| platform   | 交易平台               | Yes      | string       |
| symbols    | 需要订阅和交易的交易对 | Yes      | list[string] |
| access_key | 登录令牌               | Yes      | string       |
| secret_key | 令牌密钥               | Yes      | string       |

>响应示例

```json
    {
        "op": "login",
        "cid": "xxxxx",
        "result": true,
        "error_message": "",
        "token": "xxxxxxxxxxx"
    }
```

| Name          | Schema  | Description        |
| :------------ | :-------| :----------------- |
| result        | boolean | 是否成功           |
| error_message | string  | 错误信息           |
| token         | string  | 登录凭证           |



### 2.下单
通过此操作提交一个新的订单.  
action包括"SELL"和"BUY".  
order_type包括"LIMIT"和"MARKET"和"IOC".  
如果订单类型为市价单的话price应该设置为0.  
对于可以同时拥有多头和空头仓位的合约类型,quantity可以有正负符号,这个正负符号代表是操作多头仓位还是空头仓位

>请求示例
```json
    {
        "op": "place_order",
        "cid": "xxxxx",
        "token": "xxxxxxxxxxx",
        "symbol": "ETH/USDT",
        "action": "SELL",
        "price": 219.24,
        "quantity": 5.6,
        "order_type": "LIMIT"
    }
```

| Name       | Description  | Required | Schema |
| ---------- | ------------ | -------- | ------ |
| op         | 操作名称     | Yes      | string |
| cid        | 请求自定义ID | No       | string |
| token      | 登录凭证     | Yes      | string |
| symbol     | 交易对       | Yes      | string |
| action     | 买还是卖     | Yes      | string |
| price      | 价格         | Yes      | number |
| quantity   | 数量         | Yes      | number |
| order_type | 订单类型     | Yes      | string |

>响应示例

```json
    {
        "op": "place_order",
        "cid": "xxxxx",
        "result": true,
        "error_message": "",
        "order_no": "xxxxxxxxxx"
    }
```

| Name          | Schema  | Description        |
| :------------ | :------ | :----------------- |
| result        | boolean | 是否成功           |
| error_message | string  | 错误信息           |
| order_no      | string  | 订单ID             |



### 3.撤销订单
通过此操作可以撤销当前挂起的订单

>请求示例
```json
    {
        "op": "cancel_order",
        "cid": "xxxxx",
        "token": "xxxxxxxxxxx",
        "symbol": "ETH/USDT",
        "order_nos": ["2510832677225473","2510832677225474"]
    }
```

| Name       | Description  | Required | Schema       |
| ---------- | ------------ | -------- | ------------ |
| op         | 操作名称     | Yes      | string       |
| cid        | 请求自定义ID | No       | string       |
| token      | 登录凭证     | Yes      | string       |
| symbol     | 交易对       | Yes      | string       |
| order_nos  | 订单ID列表   | Yes      | list[string] |

>响应示例

```json
    {
        "op": "cancel_order",
        "cid": "xxxxx",
        "result": true,
        "error_message": "",
        "data": 
        [
            {
                "result": true,
                "error_message": "",
                "order_no": "2510832677225473"
            },
            {
                "result": true,
                "error_message": "",
                "order_no": "2510832677225474"
            }
        ]
    }
```

| Name          | Schema  | Description        |
| :------------ | :-------| :----------------- |
| result        | boolean | 是否成功           |
| error_message | string  | 错误信息           |
| order_no      | string  | 订单ID             |



### 4.查询当前未成交订单
通过此操作可以查询当前未成交订单

>请求示例
```json
    {
        "op": "open_orders",
        "cid": "xxxxx",
        "token": "xxxxxxxxxxx",
        "symbol": "ETH/USDT"
    }
```

| Name       | Description  | Required | Schema |
| ---------- | ------------ | -------- | ------ |
| op         | 操作名称     | Yes      | string |
| cid        | 请求自定义ID | No       | string |
| token      | 登录凭证     | Yes      | string |
| symbol     | 交易对       | Yes      | string |

>响应示例

```json
    {
        "op": "open_orders",
        "cid": "xxxxx",
        "result": true,
        "error_message": "",
        "data":	
        [
            {
                "order_no": "2510832677225473",
                "action": "BUY",
                "price": 2289.6,
                "quantity": 23,
                "remain": 11,
                "status": "PARTIAL-FILLED",
                "order_type": "LIMIT",
                "ctime": 1530604762277,
                "utime": 1530604762278
            },
            {
                "order_no": "2510832677225474",
                "action": "SELL",
                "price": 2189.6,
                "quantity": 24.6,
                "remain": 0,
                "status": "SUBMITTED",
                "order_type": "LIMIT",
                "ctime": 1530604762222,
                "utime": 1530604762311
            }
        ]
    }
```

| Name          | Schema  | Description        |
| :------------ | :-------| :----------------- |
| order_no      | string  | 订单ID             |
| action        | string  | 买卖方向           |
| price         | number  | 订单价格           |
| quantity      | number  | 订单数量           |
| remain        | number  | 剩余数量           |
| status        | string  | 状态               |
| order_type    | string  | 订单类型           |
| ctime         | number  | 创建时间           |
| utime         | number  | 更新时间           |



### 5.查询账户资产
通过此操作可以查询当前账户资产

>请求示例
```json
    {
        "op": "asset",
        "cid": "xxxxx",
        "token": "xxxxxxxxxxx"
    }
```

| Name       | Description  | Required | Schema |
| ---------- | ------------ | -------- | ------ |
| op         | 操作名称     | Yes      | string |
| cid        | 请求自定义ID | No       | string |
| token      | 登录凭证     | Yes      | string |

>响应示例

```json
    {
        "op": "asset",
        "cid": "xxxxx",
        "result": true,
        "error_message": "",
        "data":
        {
            "BTC": 
            {
                "free": 1.1, 
                "locked": 2.2, 
                "total": 3.3
            },
            "ETH": 
            {
                "free": 1.1, 
                "locked": 2.2, 
                "total": 3.3
            },
            "EOS": 
            {
                "free": 1.1, 
                "locked": 2.2, 
                "total": 3.3
            }
        }
    }
```

| Name          | Schema  | Description        |
| :------------ | :-------| :----------------- |
| free          | number  | 可用余额           |
| locked        | number  | 冻结余额           |
| total         | number  | 总余额             |



### 6.查询当前持仓
通过此操作可以查询当前持仓

>请求示例
```json
    {
        "op": "position",
        "cid": "xxxxx",
        "token": "xxxxxxxxxxx",
        "symbol": "ETH/USDT"
    }
```

| Name       | Description  | Required | Schema |
| ---------- | ------------ | -------- | ------ |
| op         | 操作名称     | Yes      | string |
| cid        | 请求自定义ID | No       | string |
| token      | 登录凭证     | Yes      | string |
| symbol     | 交易对       | Yes      | string |

>响应示例

```json
    {
        "op": "position",
        "cid": "xxxxx",
        "result": true,
        "error_message": "",
        "data": 
        {
            "margin_mode": "crossed",

            "long_quantity": 0,
            "long_avail_qty": 0,
            "long_open_price": 0,
            "long_hold_price": 0,
            "long_liquid_price": 0,
            "long_unrealised_pnl": 0,
            "long_leverage": 0,
            "long_margin": 0,

            "short_quantity": 0,
            "short_avail_qty": 0,
            "short_open_price": 0,
            "short_hold_price": 0,
            "short_liquid_price": 0,
            "short_unrealised_pnl": 0,
            "short_leverage": 0,
            "short_margin": 0,

            "utime": 1530604762311
        }
    }
```

| Name                 | Schema  | Description        |
| :------------------- | :------ | :----------------- |
| margin_mode          | string  | 全仓or逐仓         |
| long_quantity        | number  | 多仓数量           |
| long_avail_qty       | number  | 多仓可用数量       |
| long_open_price      | number  | 多仓开仓平均价格   |
| long_hold_price      | number  | 多仓持仓平均价格   |
| long_liquid_price    | number  | 多仓预估爆仓价格   |
| long_unrealised_pnl  | number  | 多仓未实现盈亏     |
| long_leverage        | number  | 多仓杠杠倍数       |
| long_margin          | number  | 多仓保证金         |
| short_quantity       | number  | 空仓数量           |
| short_avail_qty      | number  | 空仓可用数量       |
| short_open_price     | number  | 空仓开仓平均价格   |
| short_hold_price     | number  | 空仓持仓平均价格   |
| short_liquid_price   | number  | 空仓预估爆仓价格   |
| short_unrealised_pnl | number  | 空仓未实现盈亏     |
| short_leverage       | number  | 空仓杠杠倍数       |
| short_margin         | number  | 空仓保证金         |
| utime                | number  | 更新时间戳         |



### 7.查询符号信息
通过此操作可以查询指定符号信息

>请求示例
```json
    {
        "op": "symbol_info",
        "cid": "xxxxx",
        "token": "xxxxxxxxxxx",
        "symbol": "ETH/USDT"
    }
```

| Name       | Description  | Required | Schema |
| ---------- | ------------ | -------- | ------ |
| op         | 操作名称     | Yes      | string |
| cid        | 请求自定义ID | No       | string |
| token      | 登录凭证     | Yes      | string |
| symbol     | 交易对       | Yes      | string |

>响应示例

```json
    {
        "op": "symbol_info",
        "cid": "xxxxx",
        "result": true,
        "error_message": "",
        "data":
        {
            "price_tick": 0.01,
            "size_tick": 1,
            "size_limit": 10,
            "value_tick": 1,
            "value_limit": 10,
            "base_currency": "ETH",
            "quote_currency": "USDT",
            "settlement_currency": "USDT"
        }
    }
```

| Name                | Schema  | Description                 |
| :------------------ | :-------| :-------------------------- |
| price_tick          | number  | `报价`每一跳的最小单位       |
| size_tick           | number  | `下单量`每一跳的最小单位     |
| size_limit          | number  | 最小`下单量`                 |
| value_tick          | number  | `下单金额`每一跳的最小单位   |
| value_limit         | number  | 最小`下单金额`               |
| base_currency       | number  | 交易对中的基础币种           |
| quote_currency      | number  | 交易对中的报价币种           |
| settlement_currency | number  | 交易对中的结算币种           |



### 8.退出登录
通过此操作可以退出指定登录,建议客户端在关闭websocket套接字之前先退出所有登录

>请求示例
```json
    {
        "op": "logout",
        "cid": "xxxxx",
        "token": "xxxxxxxxxxx"
    }
```

| Name          | Description  | Required | Schema |
| ------------- | ------------ | -------- | ------ |
| op            | 操作名称     | Yes      | string |
| cid           | 请求自定义ID | No       | string |
| token         | 登录凭证     | Yes      | string |

>响应示例

```json
    {
        "op": "logout",
        "cid": "xxxxx",
        "result": true,
        "error_message": ""
    }
```

| Name          | Schema  | Description        |
| :------------ | :-------| :----------------- |
| result        | boolean | 是否成功           |
| error_message | string  | 错误信息           |



------------

## 下面都是被动通知(订单,成交,仓位,资产)



### 1.订单通知
属于被动通知,当用户下单成功或者订单状态有任何变化都会接收到此通知

>通知示例

```json
    {
        "op": "notify",
        "topic": "order",
        "token": "xxxxxxxxxxx",
        "data":
        {
            "symbol": "ETH/USDT",
            "order_no": "2510832677225474",
            "action": "SELL",
            "price": 2189.6,
            "quantity": 24.6,
            "remain": 0,
            "status": "SUBMITTED",
            "order_type": "LIMIT",
            "ctime": 1530604762222,
            "utime": 1530604762311
        }
    }
```



### 2.成交通知
属于被动通知,只要有订单成交就会接收到此通知

>通知示例

```json
    {
        "op": "notify",
        "topic": "fill",
        "token": "xxxxxxxxxxx",
        "data":
        {
            "symbol": "ETH/USDT",
            "order_no": "2510832677225474",
            "fill_no": "9302836",
            "price": 2189.6,
            "quantity": 24.6,
            "side": "SELL",
            "liquidity": "TAKER",
            "ctime": 1530604762311
        }
    }
```



### 3.仓位通知
属于被动通知,只要仓位有变动就会接收到此通知

>通知示例

```json
    {
        "op": "notify",
        "topic": "position",
        "token": "xxxxxxxxxxx",
        "data":
        {
            "symbol": "ETH/USDT",

            "margin_mode": "crossed",

            "long_quantity": 0,
            "long_avail_qty": 0,
            "long_open_price": 0,
            "long_hold_price": 0,
            "long_liquid_price": 0,
            "long_unrealised_pnl": 0,
            "long_leverage": 0,
            "long_margin": 0,

            "short_quantity": 0,
            "short_avail_qty": 0,
            "short_open_price": 0,
            "short_hold_price": 0,
            "short_liquid_price": 0,
            "short_unrealised_pnl": 0,
            "short_leverage": 0,
            "short_margin": 0,

            "utime": 1530604762311
        }
    }
```



### 4.资产通知
属于被动通知,只要资产有变动就会接收到此通知

>通知示例

```json
    {
        "op": "notify",
        "topic": "asset",
        "token": "xxxxxxxxxxx",
        "data":
        {
            "BTC": 
            {
                "free": 1.1, 
                "locked": 2.2, 
                "total": 3.3
            },
            "ETH": 
            {
                "free": 1.1, 
                "locked": 2.2, 
                "total": 3.3
            },
            "EOS": 
            {
                "free": 1.1, 
                "locked": 2.2, 
                "total": 3.3
            }
        }
    }
```



------------

## 状态通知
在登录交易所过程中以及在正常运行过程中发生任何问题都会经此通知客户端,下面是相应状态码:

    STATE_CODE_PARAM_MISS = 1       #交易接口初始化过程缺少参数
    STATE_CODE_CONNECT_SUCCESS = 2  #交易接口连接成功
    STATE_CODE_CONNECT_FAILED = 3   #交易接口连接失败
    STATE_CODE_DISCONNECT = 4       #交易接口连接断开
    STATE_CODE_RECONNECTING = 5     #交易接口重新连接中
    STATE_CODE_READY = 6            #交易接口准备好
    STATE_CODE_GENERAL_ERROR = 7    #交易接口常规错误

>通知示例

```json
    {
        "op": "state",
        "token": "xxxxxxxxxxx",
        "code": STATE_CODE_RECONNECTING,
        "msg": "xxxxxx"
    }
```