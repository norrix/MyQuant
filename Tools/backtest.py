# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:29:19 2015

@author: norris
"""

import os
import mysql.connector as sqlconn
import pandas as pd
import math
import datetime
import matplotlib.pyplot as plt

def ln(x):
    return math.log(x, math.e)
    
def getNameid(name):
    return "".join([a for a in name[:2] if a.isalpha()])


def getSpotid(name, cursor):
    query = 'SELECT spotid FROM futures.spotlist WHERE nameid = \'%s\' LIMIT 1' % getNameid(name)
    cursor.execute(query)
    for id in cursor:
        spotid = id[0]
    return spotid

def addSuffix(name):
    nameid = getNameid(name)
    if nameid in {'CU','AL','ZN','PB','AU','AG','RB','RU','FU','WR','BU','HC','IM'}:
        return name + '.SHF'
    if nameid in {'A','M','Y','P','C','I','JM','J','L','V','B','JD','FB','BB','PP','CS'}:
        return name + '.DCE'
    return name + '.CZC'

# 返回的价格已乘杠杆系数（价差可直接相减）
def load(nameid, start, end, coef, cursor):
    name = addSuffix(nameid)
    spotid = getSpotid(name, cursor)
    Time = []
    Futures = []
    Basis = []
    query = "SELECT * FROM \
    (SELECT a.Time Time, a.Close Futures, b.Close Spot FROM \
    (SELECT Close, Time FROM futures.prices WHERE Symbol = '%s') a JOIN \
    (SELECT Close, Time FROM futures.prices WHERE Symbol = '%s') b ON \
    a.Time = b.Time) c WHERE Time BETWEEN \
    DATE_FORMAT('%s','%%Y-%%m-%%d') AND DATE_FORMAT('%s','%%Y-%%m-%%d')"\
    % (name, spotid, start, end)
    cursor.execute(query)
    for dbTime, dbFutures, dbSpot in cursor:
        Time.append(dbTime)
        Futures.append(dbFutures*coef)
        Basis.append(dbFutures-dbSpot)
    return pd.DataFrame({nameid:Futures,nameid+'_b':Basis}, index = Time)

# 计算PNL
def calPNL(tradeList, loadData):
    pnlList = []
    for trade in tradeList:
        if trade['short'] == 1:
            pnl = ln(loadData.iloc[trade['out'],2]) - ln(loadData.iloc[trade['in'],2]) # long pnl
            pnl += ln(loadData.iloc[trade['in'],0]) - ln(loadData.iloc[trade['out'],0])# short pnl
        else:
            pnl = ln(loadData.iloc[trade['out'],0]) - ln(loadData.iloc[trade['in'],0]) # long pnl
            pnl += ln(loadData.iloc[trade['in'],2]) - ln(loadData.iloc[trade['out'],2])# short pnl
        pnlList.append(pnl)
    return pnlList

# 按照1份long/short合约计算最大回撤(ln-ln)
def maxDrawdown(loadData, short, drawdown):
    if short == 1:
        newdrawdown = ln(loadData.iloc[:,2].max()) - ln(loadData.iloc[-1,2])
        newdrawdown -= ln(loadData.iloc[:,0].min()) - ln(loadData.iloc[-1,0])
    else:
        newdrawdown = ln(loadData.iloc[:,0].max()) - ln(loadData.iloc[-1,0])
        newdrawdown -= ln(loadData.iloc[:,2].min()) - ln(loadData.iloc[-1,2])

    if newdrawdown > drawdown:
        return newdrawdown
    return drawdown

# 绘制回测
def plot(loadData, tradeList, pnlList):
    time = loadData.index
    fig = plt.figure(figsize = (12,8))
    ax1 = fig.add_subplot(311)
    ax2 = fig.add_subplot(312, sharex = ax1)
    ax3 = fig.add_subplot(313, sharex = ax1)
    
    
    ax1.plot(time, loadData.ix[:,0], label = loadData.columns[0])
    ax1.plot(time, loadData.ix[:,2], label = loadData.columns[2])
    for trade in tradeList:
        ax1.axvspan(time[trade['in']], time[trade['out']], facecolor='0.2', alpha=0.2, edgecolor  = None)
    ax1.legend(framealpha = 0.2, fontsize = 'small', ncol = 2)
    ax1.grid(True)
    
    pnlTime = time[[trade['out'] for trade in tradeList]]
    pnlSum = [sum(pnlList[:i]) for i in xrange(1,len(pnlList)+1)]
    ax2.bar(pnlTime, pnlList, color = 'm', label = 'PNL_list')
    ax2.plot(pnlTime, pnlSum, color = 'r', label = 'PNL_sum')
    ax2.legend(framealpha = 0.2, fontsize = 'small', ncol = 2)
    ax2.grid(True)
    
    
    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(ax2.get_xticklabels(), rotation = 30, horizontalalignment='right')
    plt.close(fig)






def trade(pair, cursor):
    nameid1 = pair[0]
    nameid2 = pair[1]
    coef1 = pair[2]
    coef2 = pair[3]
    start = pair[4]
    end = pair[5]
    in_sign = pair[6]
    out_sign = pair[7]
    tlim = pair[8]
    dlim = pair[9]
    loadData1 = load(nameid1, start, end, coef1, cursor)
    loadData2 = load(nameid2, start, end, coef2, cursor)
    loadData = pd.concat([loadData1, loadData2], axis = 1, join = 'inner')
    if not len(loadData):
        print('No data returned when query %s_%s from %s to %s.' % (nameid1, nameid2, start, end))
        return False
    if loadData.index[0].strftime('%Y-%m-%d') != start or loadData.index[-1].strftime('%Y-%m-%d') != end:
        print('Data of pair %s_%s start from %s to %s' % (nameid1, nameid2, start, end))
    # trade
    Basis1 = list(loadData[nameid1+'_b'])
    Basis2 = list(loadData[nameid2+'_b'])
    hold = False
    tradeList = []
    drawdown = 0
    for i in xrange(len(loadData)):
        if not hold:
             #有基差出入场信号
            if Basis1[i] >= in_sign[0] or Basis2[i] >= in_sign[1]: # 超过上阈值
                if Basis1[i]-in_sign[0] >= Basis2[i]-in_sign[1]:   # 1超过阈值更多
                    trigger = 1; short = 1                         # Short 1
                else:                                              # 2超过阈值更多
                    trigger = 2; short = 2                         # Short 2
                in_time = i; hold = True; continue
            if Basis1[i] <= in_sign[2] or Basis2[i] <= in_sign[3]: # 超过下阈值
                if Basis1[i]-in_sign[2] >= Basis2[i]-in_sign[3]:   # 2超过阈值更多
                    trigger = 2; short = 1                         # Long 2
                else:                                              # 1超过阈值更多
                    trigger = 1; short = 2                         # Long 1
                in_time = i; hold = True; continue
        
        #检查出场信号
        if hold and out_sign != None:
            if (trigger == 1 and short == 1 and Basis1[i] < out_sign[0]) or\
               (trigger == 1 and short == 2 and Basis1[i] > out_sign[1]) or\
               (trigger == 2 and short == 2 and Basis2[i] < out_sign[2]) or\
               (trigger == 2 and short == 1 and Basis2[i] > out_sign[3]):
                tradeList.append({'in':in_time, 'out':i, 'short':short}); hold = False
        #检查持有时间
        if hold and tlim != None and i - in_time == tlim:
            tradeList.append({'in':in_time, 'out':i, 'short':short}); hold = False
        #检查最大回撤
        if hold and dlim != None:
            drawdown = maxDrawdown(loadData[in_time:i], short, drawdown)
            if drawdown >= dlim:
                tradeList.append({'in':in_time, 'out':i, 'short':short}); hold = False
    # 计算PNL
    pnlList = calPNL(loadData, tradeList, loadData)
    return plot(tradeList, pnlList)
                


def backtest(watchlist):
    conn = sqlconn.connect(user='root', password='root', host='10.0.1.130')
    cursor = conn.cursor()
    filePath = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    if not os.path.isdir('Report'):
        os.mkdir('Report')
    os.chdir('Report')
    os.mkdir(filePath)
    os.chdir(filePath)
    f = open('report.txt','a')
    for i in xrange(len(watchlist)):
        pair = watchlist[i]
        success = trade(pair, cursor)
        if success:
            plot(tradeData, i)
    f.close()


if __name__ == '__main__':
    watchlist = [\
        # nameid1, nameid2, coef1, coef2, start, end, in_sign(basis), out_sign(basis), time_limit, drawdown
        # ['CU1504', 'CU1505', 1, 1, '2014-04-15', '2014-09-30', [25, -25], None, None, None, None],\
        ['CU1504', 'CU1505', 1, 1, '2014-04-15', '2014-09-30', [25, -25, 25, -25], [15, -15, 15, -15], 10, None],\
        ['CU1504', 'CU1505', 1, 1, '2014-10-01', '2014-10-30', [90, -90, 90, -90], [90, -90, 90, -90], 10, 40]
    ]
    backtest(watchlist)
