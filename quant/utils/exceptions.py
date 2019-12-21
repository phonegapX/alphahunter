# -*- coding:utf-8 -*-

"""
Error/Exception definition.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""


class CustomException(Exception):
    default_msg = "A server error occurred."
    default_data = None
    default_code = 500

    def __init__(self, msg=None, code=None, data=None):
        self.msg = msg if msg is not None else self.default_msg
        self.code = code if code is not None else self.default_code
        self.data = data

    def __str__(self):
        str_msg = "[{code}] {msg}".format(code=self.code, msg=self.msg)
        return str_msg


class ValidationError(CustomException):
    default_msg = "Bad Request"
    default_code = 400


class NotAuthenticated(CustomException):
    default_msg = "Unauthorized"
    default_code = 401


class AuthenticationFailed(CustomException):
    default_msg = "Forbidden"
    default_code = 403


class NotFound(CustomException):
    default_msg = "Not found"
    default_code = 404


class SystemException(CustomException):
    default_msg = "Internal Server Error"
    default_code = 500


class TimeoutException(CustomException):
    default_msg = "Timeout"
    default_code = 504


class GlobalLockerException(CustomException):
    default_msg = "Global Locker Timeout"
