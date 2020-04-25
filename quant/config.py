# -*- coding:utf-8 -*-

"""
Config module.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import json

from quant.utils import tools
from quant.utils import logger


class Config:
    """ Config module will load a json file like `config.json` and parse the content to json object.
        1. Configure content must be key-value pair, and `key` will be set as Config module's attributes;
        2. Invoking Config module's attributes cat get those values;
        3. Some `key` name is upper case are the build-in, and all `key` will be set to lower case:
            SERVER_ID: Server id, every running process has a unique id.
            RUN_TIME_UPDATE: If this server support run-time-update(receive EventConfig), True or False, default is False.
            LOG: Logger print config.
            RABBITMQ: RabbitMQ config, default is None.
            MONGODB: MongoDB config, default is None.
            REDIS: Redis config, default is None.
            PLATFORMS: Trading Exchanges config, default is [].
            ACCOUNTS: Trading Exchanges config list, default is [].
            MARKETS: Market Server config list, default is {}.
            HEARTBEAT: Server heartbeat config, default is {}.
            PROXY: HTTP proxy config, default is None.
            BACKTEST: Strategy backtest config, default is {}.
            DATAMATRIX: Data matrix config, default is {}.
    """

    def __init__(self):
        self.server_id = None
        self.run_time_update = False
        self.log = {}
        self.rabbitmq = {}
        self.mongodb = {}
        self.redis = {}
        self.platforms = []
        self.accounts = []
        self.markets = {}
        self.heartbeat = {}
        self.proxy = None
        self.backtest = {}
        self.datamatrix = {}

    def register_run_time_update(self):
        """Subscribe EventConfig and that can update config in run-time dynamically."""
        if self.run_time_update:
            from quant.event import EventConfig
            EventConfig(self.server_id).subscribe(self._on_event_config)

    def loads(self, config_file=None):
        """ Load config file.

        Args:
            config_file: config json file.
        """
        configures = {}
        if config_file:
            try:
                with open(config_file) as f:
                    data = f.read()
                    configures = json.loads(data)
            except Exception as e:
                print(e)
                exit(0)
            if not configures:
                print("config json file error!")
                exit(0)
        self._update(configures)

    async def _on_event_config(self, data):
        """ Config event update.

        Args:
            data: New config received from ConfigEvent.
        """
        server_id = data["server_id"]
        params = data["params"]
        if server_id != self.server_id:
            logger.error("Server id error:", server_id, caller=self)
            return
        if not isinstance(params, dict):
            logger.error("params format error:", params, caller=self)
            return

        params["SERVER_ID"] = self.server_id
        params["RUN_TIME_UPDATE"] = self.run_time_update
        self._update(params)
        logger.info("config update success!", caller=self)

    def _update(self, update_fields):
        """ Update config attributes.

        Args:
            update_fields: Update fields.
        """
        self.server_id = update_fields.get("SERVER_ID", tools.get_uuid1())
        self.run_time_update = update_fields.get("RUN_TIME_UPDATE", False)
        self.log = update_fields.get("LOG", {})
        self.rabbitmq = update_fields.get("RABBITMQ", {})
        self.mongodb = update_fields.get("MONGODB", {})
        self.redis = update_fields.get("REDIS", {})
        self.platforms = update_fields.get("PLATFORMS", [])
        self.accounts = update_fields.get("ACCOUNTS", [])
        self.markets = update_fields.get("MARKETS", {})
        self.heartbeat = update_fields.get("HEARTBEAT", {})
        self.proxy = update_fields.get("PROXY", None)
        self.backtest = update_fields.get("BACKTEST", {})
        self.datamatrix = update_fields.get("DATAMATRIX", {})

        for k, v in update_fields.items():
            setattr(self, k, v)


config = Config()
