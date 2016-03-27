# -*- coding: utf-8 -*-

class DatabaseError(Exception):
    # 数据库连接异常
    pass

class ParseError(Exception):
    # 解析数据异常
    pass

class InvalidParamError(Exception):
    # 参数异常
    pass