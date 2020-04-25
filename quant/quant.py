# -*- coding:utf-8 -*-

"""
Asynchronous driven quantitative trading framework.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import signal
import asyncio

from quant.utils import logger
from quant.config import config


class Quant:
    """ Asynchronous driven quantitative trading framework.
    """

    def __init__(self):
        self.loop = None
        self.event_center = None

    def initialize(self, config_module=None):
        """ Initialize.

        Args:
            config_module: config file path, normally it"s a json file.
        """
        self._get_event_loop()
        self._load_settings(config_module)
        self._init_logger()
        self._init_db_instance()
        self._init_event_center()
        self._do_heartbeat()

    def start(self):
        """Start the event loop."""
        def keyboard_interrupt(s, f):
            print("KeyboardInterrupt (ID: {}) has been caught. Cleaning up...".format(s))
            self.loop.stop()
        signal.signal(signal.SIGINT, keyboard_interrupt)

        logger.info("start io loop ...", caller=self)
        self.loop.run_forever()

    def stop(self):
        """Stop the event loop."""
        logger.info("stop io loop.", caller=self)
        self.loop.stop()

    def _get_event_loop(self):
        """ Get a main io loop. """
        if not self.loop:
            self.loop = asyncio.get_event_loop()
        return self.loop

    def _load_settings(self, config_module):
        """ Load config settings.

        Args:
            config_module: config file path, normally it"s a json file.
        """
        config.loads(config_module)

    def _init_logger(self):
        """Initialize logger."""
        console = config.log.get("console", True)
        level = config.log.get("level", "DEBUG")
        path = config.log.get("path", "/tmp/logs/Quant")
        name = config.log.get("name", "quant.log")
        clear = config.log.get("clear", False)
        backup_count = config.log.get("backup_count", 0)
        if console:
            logger.initLogger(level)
        else:
            logger.initLogger(level, path, name, clear, backup_count)

    def _init_db_instance(self):
        """Initialize db."""
        if config.mongodb:
            from quant.utils.mongo import MongoDB
            MongoDB.mongodb_init(**config.mongodb)

    def _init_event_center(self):
        """Initialize event center."""
        if config.rabbitmq and not config.backtest and not config.datamatrix:
            from quant.event import EventCenter
            self.event_center = EventCenter()
            self.event_center.initialize()
            config.register_run_time_update()

    def _do_heartbeat(self):
        """Start server heartbeat."""
        from quant.heartbeat import heartbeat
        self.loop.call_later(0.5, heartbeat.ticker)


quant = Quant()
