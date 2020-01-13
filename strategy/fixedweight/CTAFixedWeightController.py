# -*- coding:utf-8 -*-

"""
固定权重模式CTA主策略演示

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

class CTAFixedWeightController(Strategy):

	def __init__(self):
		""" 初始化
        """
        super(CTAFixedWeightController, self).__init__()

        # 指定这个主策略下面管理了CTAFixedWeightModel1和CTAFixedWeightModel2这两个模型
        self.models = [CTAFixedWeightModel1, CTAFixedWeightModel2]

        # CTAFixedWeightModel1订阅‘btcusdt’， CTAFixedWeightModel2订阅‘btcusdt’，所以需要在主策略里面同时订阅这一种
        self.symbols = ['btcusdt']

        # CTAFixedWeightModel1关心‘btc’， CTAFixedWeightModel2关心‘btcusdt’，所以需要在主策略里面同时订阅这一种
        self.coins = ['btc']

        # 主策略模式：指定这个主策略是采用fixed_weight模式
        self.mode = 'fixed_weight'

        # 主策略计价货币：指定这个主策略是以usdt为计价货币
        self.quote_asset = 'usdt'

        # 主策略计价货币在账户中的最低比例：指定这个主策略在运行过程中，需要保证手上要有至少5%的现金在手上，不允许所有资产全部用来买货物
        self.quote_asset_fraction = 0.05

        # 主策略所对应的账户的资产数值，计算时候要以self.quote_asset计价货币统一折算，比如账户里现在有BTC 10个， ETH 20个，USDT 231个， 
        # 如果当前BTC/USDT价格为8000， ETH/USDT价格为160， 那么当前self.asset_market_value = 10 * 8000 + 20 * 160 + 231 = 83431USDT
        self.asset_market_value = None
 
      	# 主策略当前仓位，这是一个字典类型，key是账户中我们关心的那10种主流币种，注意，这里面只包含我们认为是货的币种，不包含我们认为是钱的币种，
      	# 比如，一个客户给了我们一些USDT，那么我们认为，这个USDT就是我们的钱，即计价货币，self.quote_asset, 那么self.current_position这个字典，就不会包含usdt这个key
      	# 再比如，一个客户给了我们一些BTC，那么我们认为，这个BTC就是我们的钱，即计价货币，self.quote_asset, 那么self.current_position这个字典，就不会包含btc这个key
      	# 这个字典的value也是一个字典，第一个值是该币种的个数，第二个值是该币种对应self.quote_asset折算的市值
      	# 比如账户里现在有BTC 10个， ETH 20个，USDT 231个，如果当前BTC/USDT价格为8000， ETH/USDT价格为160，那么：
      	# 这个self.current_position = {
        #                            	'btc': {'number': 10.0, 'market_value': 10.0*8000},
        #                          		'eth': {'number': 20.0, 'market_value': 20.0*160},
        #                          		'total': {market_value': 10.0*8000 + 20.0*160}
        #							 }
        self.current_position = {
                                  'btc': {'number': None, 'market_value': None},
                                  'eth': {'number': None, 'market_value': None},
                                  'total': {'market_value': None, 'absolute_market_value': None}
        						}

        # 主策略目标仓位，这个变量的值也是一个字典类型，计算同self.current_position
        self.target_position = {
                                  'btc': {'number': None, 'market_value': None},
                                  'eth': {'number': None, 'market_value': None},
                                  'total': {'market_value': None, 'absolute_market_value': None}
        						}

        # 主策略差值仓位，即需要调仓的部分：当前仓位距离目标仓位的差距
        self.delta_position = {
                                  'btc': {'number': None, 'market_value': None},
                                  'eth': {'number': None, 'market_value': None},
                                  'total': {'market_value': None, 'absolute_market_value': None}
        					   }

        # 主策略下面管理model的最新状态，是字典类型，key是一个model，value是一个字典，
        # 以CTAFixedWeightModel1为例，value这个字典中，
        # symbol代表这个model订阅的交易对，
        # coins代表这个model关心的货的种类，
        # mode代表这个model的模式（固定权重模式），
        # mode_params是这个固定权重模式下的具体参数，字典类型，其中fixed_weight为固定权重的具体数值，max_money为该模型占用资金的最大上限
        # status是这个model的最新状态，字典类型，其中，
        # model_signals_want是这个model自己产生的信号想要的仓位，字典类型，key是币种，value包含model给出的原始signal，币种数量，币种市值（当然还是以self.quote_asset折算），
        # strategy_give指的是经过主策略汇总各model的signal之后，决定真实给每个model的仓位，这是经历了一个叫做模型组合(model_consolidation)的过程而得到的。
        # 字典类型，key是币种，value包含策略给model反馈的feedback_signal，币种数量，币种市值（当然还是以self.quote_asset折算）
        # running_status是这个model的运行状态，取值为running_free或者running_limited, 在模型组合过程中，如果主策略给model分配的仓位完全等于model自己想要的，
        # 那么状态就为running_free, 如果model想要的超过了max_money限制，主策略就会按照max_money给这个model，这种情况，状态就变成了running_limited
        self.models_info = {
        					CTAFixedWeightModel1: {
        											 'symbols': ['btcusdt'],
        											 'coins': ['btc'],
        											 'mode': 'fixed_weight',
        											 'mode_params': {
        											                  'fixed_weight': 0.3, 
        											                  'max_money': 120
        											                 },
        											 'status':  {
        											 				'model_signal_want': {
						        											               'btc': {
						        											               			'raw_signal': None,
						        											                       }
							        											 		   },

							        								'strategy_give': {
					        											               'btc': {
					        											               			'feedback_signal': None,
					        											                        'number': None, 
					        											                        'market_value': None
					        											                       }
						        											 		  },

						        									'running_status': 'running_free' or 'running_limited'

        											             }
        										   }

        					CTAFixedWeightModel2: {
        											 'symbols': ['btcusdt'],
        											 'coins': ['btc'],
        											 'mode': 'fixed_weight',
        											 'mode_params': {
        											                  'fixed_weight': 0.7, 
        											                  'max_money': 480
        											                 },
        											 'status':  {
        											 				'model_signal_want': {
						        											               'btc': {
						        											               			'raw_signal': None,
						        											                       }
							        											 		   },

							        								'strategy_give': {
					        											               'btc': {
					        											               			'feedback_signal': None,
					        											                        'number': None, 
					        											                        'market_value': None
					        											                       }
						        											 		  },

						        									'running_status': 'running_free' or 'running_limited'

        											             }
        										   }					   

        				   }


    def init_models_info(self):
    	for model in self.models:
    		self.models_info[model] = {}
    		self.models_info[model][symbols] = model.symbols
    		self.models_info[model][coins] = model.coins
    		self.models_info[model][mode] = model.mode 
    		self.models_info[model][mode_params] = model.mode_params
    		self.models_info[model][status] = {
    											 'model_signal_want': {},
    											 'strategy_give': {},
    											 'running_statsu': None
    										   }
    		for coin in model.coins:
    			 self.models_info[model][status]['model_signal_want'][coin] = {'raw_signal': None}
    			 self.models_info[model][status]['strategy_give'][coin] = {'raw_signal': None, 'number': None, 'market_value': None}
    			 self.models_info[model][status]['running_status'] = None





    def update_asset_and_current_position(self):
    	''' 
    	更新账户的资产状况，以及当前仓位，
    	并计算最新的self.asset_market_value，和self.current_position
    	'''
    	pass

    def model_consolidation(self，can_use_asset_market_value, price_dict):
    	'''
		模型组合
    	'''
    	money_dict = {}
    	for model in self.models:
    		self.models_info[model][status] = {'model_signal_want': {}, 'strategy_give': {}, 'running_status': 'running_free'}

    		# model当前可用资产 = 总可用资产 * 各个model的权重
    		money1 = can_use_asset_market_value * model.mode_params['fixed_weight']
    		for coin in model.coins:

    			# model需要的目标市值 = min（model当前可用的资产 * model自己的signal值，model可用的最大钱数上线）
    			money2 = min(abs(money1 * model.signal[coin]), model.mode_params['max_money']) * sign(money1 * model.signal[coin])
    			
    			# 更新self.models_info
    			self.models_info[model][status]['model_signal_want'][coin]['raw_signal'] = model.signal[coin]
    			self.models_info[model][status]['strategy_give'][coin]['market_value'] = money2
    			self.models_info[model][status]['strategy_give'][coin]['number'] = money2 / price_dict[coin]
    			self.models_info[model][status]['strategy_give'][coin]['feedback_signal'] = money2 / money1
    			if money1 > money2 > 0.0 or money1 < money2 < 0.0:
    				self.models_info[model][status]['running_status'] = 'running_limited'

    			# 计算累计仓位市值
    			if coin not in money_dict:
    				money_dict[coin] = 0.0
    			money_dict[coin] += money2

    	# 计算整体的目标仓位
 		self.target_position = {}
 		market_value = 0.0  
 		absolute_market_value = 0.0	
    	for k, v in money_dict.items():
    		self.target_position[k] = {
    									'number': v / price_dict[k],
    									'market_value': v
    								   }
    		market_value += v
    		absolute_market_value += abs(v)
    	self.target_position['total'] = {
    									'market_value': market_value,
    									'absolute_market_value': absolute_market_value
    								   }

    	# 计算当前仓位距离目标仓位的距离，即调仓目标
    	market_value = 0.0  
 		absolute_market_value = 0.0
    	for k, v in self.target_position.items():
    		self.delta_position[k]['number'] = self.target_position[k]['number'] - self.current_position[k]['number']
    		self.delta_position[k]['market_value'] = self.target_position[k]['market_value'] - self.current_position[k]['market_value']
    		market_value += self.delta_position[k]['market_value']
 			absolute_market_value += abs(self.delta_position[k]['market_value'])
 		self.delta_position['total'] = {
    									'market_value': market_value,
    									'absolute_market_value': absolute_market_value
    								   }




    async def on_klines_combined_update_callback(self, kline_dict):
        """ 市场K线集体更新, 因为是我们使用系统自己合成的k线，我们认为不同币种在合成完成的时间是差不多的，这个方法会在集体合成完成时刻，统一推送出来
        """
        # 根据各币种最新的k线信息，得到各币种最新价格序列
        price_dict = {'btcusdt': 8000.0}

        # 获取账户信息，更新账户总市值，更新当前仓位字典
        self.update_asset_and_current_position()

        # 对model循环，获取model最新信号
        for model in self.models:
        	signal = model.on_klines_combined_update_generate_signal()

        # 根据最新的资产市值，排除需要留现金的比例，计算可被占用仓位的市值部分
        can_use_asset_market_value = self.asset_market_value * (1.0 - self.quote_asset_fraction)

        # 调用model_consolidation, 更新目标仓位等信息
        self.model_consolidation(can_use_asset_market_value, price_dict)

        # 对监控变量画图
        self.plot()

        # 根据差值仓位来下单
        self.submit_orders(self.delta_position)

        logger.info("kline:", kline, caller=self)


async def on_time(self, 5s):
        
        # 获取最新价格，得到各币种最新价格序列
        price_dict = {'btcusdt': 8000.0}

        # 获取账户信息，更新账户总市值，更新当前仓位字典
        self.update_asset_and_current_position()

        # 对model循环，获取model最新信号
        for model in self.models:
        	signal = model.on_time_update_generate_signal()

        # 根据最新的资产市值，排除需要留现金的比例，计算可被占用仓位的市值部分
        can_use_asset_market_value = self.asset_market_value * (1.0 - self.quote_asset_fraction)

        # 调用model_consolidation, 更新目标仓位等信息
        self.model_consolidation(can_use_asset_market_value, price_dict)

        # 对监控变量画图
        self.plot()

        # 根据差值仓位来下单
        self.submit_orders(self.delta_position)

        logger.info("kline:", kline, caller=self)


     async def submit_orders(self):
     	''' 根据当前最新的self.delta_position来执行下单操作 '''
     	pass

     def plot(self):
     	'''
			1. self.asset_market_value
			2. self.current_position['total']['market_value'] / self.asset_market_value
			3. self.current_position['total']['absolute_market_value'] / self.asset_market_value
			4. self.target_position['total']['market_value'] / self.asset_market_value
			5. self.target_position['total']['absolute_market_value'] / self.asset_market_value
			6. self.delta_position['total']['absolute_market_value'] / self.target_position['total']['absolute_market_value']
			7. self.models_info展示

     	'''






