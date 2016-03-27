# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 15:04:36 2016

@author: norrix
"""

import sqlite3
import math
from WindPy import w
from datetime import datetime, timedelta
from .exceptions import DatabaseError, ParseError, InvalidParamError
from .lib import *

def check_in_db(cur, code):
    sql = "select time from `fund_prices` where id = '%s' order by time desc" % code
    # print(sql)
    cur.execute(sql)
    re = cur.fetchone()
    return str(datetime.strptime(re[0], '%Y-%m-%d %H:%M:%S').date()) if re else False

def insert_data(cur, barData, tablename, stockname):
    if barData.ErrorCode == -40520007:
        raise InvalidParamError('No content in barData!')
    length = len(barData.Data[0])
    if tablename == 'fund_prices':
        print('Insert %s(%s) into %s...' % (stockname, barData.Codes[0], tablename))
        sql = "replace into `fund_prices` values (?, ?, ?)"
        args = []
        for i in range(length):
            if barData.Times[i].hour == 15 or math.isnan(barData.Data[0][i]):
                continue
            args.append((barData.Codes[0], barData.Times[i].strftime('%Y-%m-%d %H:%M:%S'), barData.Data[0][i]))
        cur.executemany(sql, args)
        print('Complete.')

def retrieve_data(cur, fundAcode, fundBcode, fundcode, startdate, enddate):
    sql = "select * from \
    (select time, price Aprice from `fund_prices` where id = '%s') a \
    inner join (select time, price Bprice from `fund_prices` where id = '%s') b using (time) \
    inner join (select time, price Iprice from `fund_prices` where id = '%s') c using (time) \
    where a.time > '%s 00:00:00' and b.time < '%s 23:59:59' \
    order by time asc" % (fundAcode, fundBcode, fundcode, startdate, enddate)
    cur.execute(sql)
    return cur.fetchall()
    
              
class StructFund(object):
    def __init__(self, fund_info):
        # param: fund_info = ('160417.OF', '华安沪深300', 'HS300A', 'HS300B')
        self.today = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
        w.start()
        infoDate = previousWorkday(self.today)
        re = w.wsd(fund_info[0],"fund_trackindexcode,fund_benchindexcode,fund_setupdate,\
        fund_managementfeeratio,fund_custodianfeeratio,fund_purchasefee,\
        fund_redemptionfee,fund_smfacode,fund_smfbcode",infoDate,infoDate,\
        "chargesType=0;Fill=Previous")
        # fund_trackindexcode       跟踪指数代码    '000300.SH'
        # fund_benchindexcode       基准指数代码    '160417BI.WI'
        # fund_setupdate            基金成立日      datetime.datetime(2012, 6, 25)
        # fund_managementfeeratio   管理费率        1.0
        # fund_custodianfeeratio    托管费率        0.22
        # fund_purchasefee          申购费率        '500万元以上 1000元/笔\r...'
        # fund_redemptionfee        赎回费率        None? (0.5%)
        # fund_smfacode             分级基金优先级  '150104.SZ'
        # fund_smfbcode             分级基金普通级  '150105.SZ'
        
        self.fundcode = fund_info[0]
        self.fundname = fund_info[1]
        self.fundAname = fund_info[2]
        self.fundBname = fund_info[3]
        
        self.trackcode = re.Data[0][0]
        self.benchcode = re.Data[1][0]
        managementfeeratio = re.Data[3][0]/100/365
        custodianfeeratio = re.Data[4][0]/100/365
        purchasefee = 0
        redemptionfee = 0.5/100
        self.feeratio = managementfeeratio + custodianfeeratio + purchasefee + redemptionfee
        self.fundAcode = re.Data[7][0]
        self.fundBcode = re.Data[8][0]
        re = w.wsd(self.fundAcode,"fund_setupdate",infoDate,infoDate,\
        "chargesType=0;Fill=Previous")
        self.setupdate = re.Data[0][0].strftime('%Y-%m-%d')

    def connect_db(self, dbname):
        self.conn = sqlite3.connect(dbname)
    
    def close_db(self):
        self.conn.close()
        
    def get_data(self):
        if not self.conn:
            raise DatabaseError('Database connecting failed.')
        cur = self.conn.cursor()
        # 如果有这几个的数据，就取最新一天的，否则取5min bar close
        lastdate = check_in_db(cur, self.fundcode)
        if not lastdate:
            startdate = nextWorkday(datetime.today() - timedelta(365*3-1))
            if isDateEarlier(startdate, self.setupdate):
                self.startdate = self.setupdate
            else:
                self.startdate = startdate
        else:
            startdate = nextWorkday(lastdate)
            if isDateEarlier(startdate, self.today):
                self.startdate = startdate
            else:
                return True
        print('Getting data of %s ... (from %s to %s)' % (self.fundname, self.startdate, self.today))
        fundAData = w.wsi(self.fundAcode,'close',self.startdate+' 09:00:00',self.today+' 15:00:00','BarSize=5')
        insert_data(cur, fundAData, 'fund_prices', 'fundA') # 150104.SZ
        fundBData = w.wsi(self.fundBcode,'close',self.startdate+' 09:00:00',self.today+' 15:00:00','BarSize=5')
        insert_data(cur, fundBData, 'fund_prices', 'fundB') # 150105.SZ
        trackData = w.wsi(self.trackcode,'close',self.startdate+' 09:00:00',self.today+' 15:00:00','BarSize=5')
        #trackData = w.wsi('000300.SH','close','2016-03-01 09:00:00','2016-03-06 15:00:00','BarSize=5')
        netData = w.wsd(self.fundcode, 'nav', self.startdate, self.today) # 160417.OF
        T = 48
        if len(trackData.Data[0])/len(netData.Data[0]) != T:
            if (len(trackData.Data[0])+1)/len(netData.Data[0]) == T and trackData.Times[-1].hour == 14 and trackData.Times[-1].minute == 55: # 缺少最后一个数据
                trackData.Times.append(trackData.Times[-1] + timedelta(minutes = 5))
                trackData.Data[0].append(trackData.Data[0][-1])
            else:
                raise ParseError('The length of track index data is not %d times that of net value data. (%d/%d)' % (T, len(trackData.Data[0]), len(netData.Data[0])))
        for k in range(len(netData.Data[0])):
            for i in range(T)[::-1]:
                trackData.Data[0][k*T+i] = trackData.Data[0][k*T+i] / trackData.Data[0][k*T] * netData.Data[0][k]
        trackData.Codes = [self.fundcode]
        insert_data(cur, trackData, 'fund_prices', 'fundIndex')
        self.conn.commit()
        cur.close()

        
    def backtest(self, startdate=None, enddate=None, premium=True, discount=True, trackingrisk=0.01, basisrisk=0.05, expectrtn=0.02):
        self.get_data()
        cur = self.conn.cursor()
        lastdate = check_in_db(cur, self.fundcode)
        if not startdate:
            startdate = lastdate
        else:
            startdate = lastdate if isDateEarlier(lastdate, startdate) else startdate
        if not enddate:
            enddate = self.today
        datedelta = datetime.strptime(enddate, '%Y-%m-%d') - datetime.strptime(startdate, '%Y-%m-%d')
        if datedelta < timedelta(2):
            raise InvalidParamError('No enough date for backtest. (from %s to %s)' % (startdate, enddate))
        
        dataSeries = retrieve_data(cur, self.fundAcode, self.fundBcode, self.fundcode, startdate, enddate)
        # [('2013-03-18 09:35:00', 1.052, 1.052, 1.021), ...]
        if not dataSeries:
            raise DatabaseError('No data retrived from database. (%s, from %s to %s)' % (self.fundcode, startdate, enddate))
        signal = trackingrisk + basisrisk + expectrtn + self.feeratio
        f = open(self.fundname + '.txt', 'w+')
        f.write('%s(%s)\t%s(%s)\t%s(%s)\n' % (self.fundAname, self.fundAcode, self.fundBname, self.fundBcode, self.fundname, self.fundcode))
        f.write('trade signal: %.2f%%\n' % (signal*100))
        f.write('backtest from %s to %s:\n' % (startdate, enddate))
        trade = None # (tradetype(premium:1, discount:2), buytimeindex, selltime(datetime), pdrate)
        tradeList = []
        #tradeList = []
        for i in range(len(dataSeries)-47*3-1): # 留三天
            data = dataSeries[i]
            time = datetime.strptime(data[0], '%Y-%m-%d %H:%M:%S')
            if trade:
                if time < trade[2]: # before selltime
                    continue
                else: # sell
                    if trade[0] == 1: # tradetype == premium
                        # T日14:55申购母基金（T+1数据），T+1日确认，T+2日分拆，T+3日开盘卖出
                        rtn = (data[1]+data[2])/2 / dataSeries[trade[1]+1][3] - 1 - self.feeratio
                        #pdrate = ((dataSeries(trade[1])[1]+dataSeries(trade[1])[2])/2/dataSeries(trade[1])[3]-1)
                        f.write(dataSeries[trade[1]][0] + \
                                ' 申购母基金，此时母基金净值参考值为%.3f' % (dataSeries[trade[1]][3]) + \
                                '，整体折溢价率达到%.2f%%' % (trade[3]*100) + \
                                '，申购时其实际净值为' + str(dataSeries[trade[1]+1][3]) + \
                                '，并于 ' + data[0] + ' 卖出分级基金AB，价格分别为' + \
                                str(data[1]) + '和' + str(data[2]) + \
                                '，减去杂费后本次套利收益为%.2f%%\n' % (rtn*100))
                    else: # tradetype == discount
                        # T日购买分级AB，T+1日确认母基金，T+2日以当日净值赎回（T+3数据）
                        rtn = dataSeries[i+1][3] / ((data[1]+data[2])/2) - 1- self.feeratio
                        f.write(data[0] + ' 买入分级基金AB，价格分别为' + \
                                str(data[1]) + '和' + str(data[2]) + \
                                '，此时母基金净值参考值为%.3f' % (dataSeries[trade[1]][3]) + \
                                '，整体折溢价率达到%.2f%%' % (trade[3]*100) + \
                                '，并于 ' + time.strftime('%Y-%m-%d') + ' 赎回母基金' + \
                                '，赎回时其实际净值为' + str(dataSeries[i+1][3]) + \
                                '，减去杂费后本次套利收益为%.2f%%\n' % (rtn*100))
                    tradeList.append({'time':time,'rtn':rtn,'type':trade[0],'target':self.fundname})
                    trade = None
            else:
                if premium and (data[1]+data[2])/2 / data[3] > 1 + signal and time.hour == 14 and time.minute >= 55: # 整体溢价
                    # 14:55溢价进场
                    trade = (1, i, datetime.strptime(dataSeries[i+47*3][0][:10] + ' 09:35:00', '%Y-%m-%d %H:%M:%S'), (data[1]+data[2])/2 / data[3] - 1)
                elif discount and data[3] / (data[1]+data[2])/2 > 1 + signal: # 整体折价
                    trade = (2, i, datetime.strptime(dataSeries[i+47*2][0][:10] + ' 14:55:00', '%Y-%m-%d %H:%M:%S'), data[3] / (data[1]+data[2])/2 - 1)
        f.close()
        cur.close()
        self.close_db()
        self.tradeList = tradeList
                
        

        
    
    