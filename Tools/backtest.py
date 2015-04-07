# -*- coding: gbk -*-
"""
Created on Fri Mar 20 14:29:19 2015

@author: norris
"""

import os
import math
import datetime
import pandas as pd
import numpy as np
import mysql.connector as sqlconn
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
        Basis.append(dbSpot-dbFutures)
    return pd.DataFrame({nameid:Futures,nameid+'_b':Basis}, index = Time)

# 计算PNL
def calPNL(loadData, tradeList):
    pnlList = []
    pnlDaily = pd.DataFrame({'sum':np.zeros(len(loadData))}, index = loadData.index)
    for trade in tradeList:
        if trade['short'] == 1:
            pnl = ln(loadData.iloc[trade['out'],2]) - ln(loadData.iloc[trade['in'],2]) # long pnl
            pnl += ln(loadData.iloc[trade['in'],0]) - ln(loadData.iloc[trade['out'],0])# short pnl
            pnlArray = np.diff(loadData.iloc[trade['in']:trade['out']+1,2].apply(ln))
            pnlArray -= np.diff(loadData.iloc[trade['in']:trade['out']+1,0].apply(ln))
        else:
            pnl = ln(loadData.iloc[trade['out'],0]) - ln(loadData.iloc[trade['in'],0]) # long pnl
            pnl += ln(loadData.iloc[trade['in'],2]) - ln(loadData.iloc[trade['out'],2])# short pnl
            pnlArray = np.diff(loadData.iloc[trade['in']:trade['out']+1,0].apply(ln))
            pnlArray -= np.diff(loadData.iloc[trade['in']:trade['out']+1,2].apply(ln))
        pnlList.append(pnl)
        if trade['in']+1 == trade['out']:
            pnlDF = pd.DataFrame({'pnl':pnlArray},index = [loadData.index[trade['out']]])
        else:
            pnlDF = pd.DataFrame({'pnl':pnlArray},index = loadData.index[trade['in']+1:trade['out']+1])
        pnlDaily = pd.concat([pnlDaily, pnlDF], axis = 1, join = 'outer')
    return pnlList, pnlDaily.sum(axis = 1)

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
def plot(loadData, tradeList, pnlList, pnlSum, in_sign, name):
    time = loadData.index
    fig = plt.figure(figsize = (25,15))
    ax1 = fig.add_subplot(311)
    ax2 = fig.add_subplot(312, sharex = ax1)
    ax3 = fig.add_subplot(313, sharex = ax1)
    
    ax1.set_title('Basis', fontsize = 14)
    ax1.plot(time, loadData.ix[:,1], label = loadData.columns[1])
    ax1.plot(time, loadData.ix[:,3], label = loadData.columns[3])
    ax1.axhspan(in_sign[0], in_sign[1], facecolor='0.2', alpha=0.2, edgecolor = None)
    ax1.axhspan(in_sign[2], in_sign[3], facecolor='0.2', alpha=0.2, edgecolor = None)
    ax1.legend(framealpha = 0.2, fontsize = 'small', ncol = 2)
    ax1.grid(True)
    
    ax2.set_title('Price', fontsize = 14)
    ax2.plot(time, loadData.ix[:,0], label = loadData.columns[0])
    ax2.plot(time, loadData.ix[:,2], label = loadData.columns[2])
    for trade in tradeList:
        ax2.axvspan(time[trade['in']], time[trade['out']], facecolor='0.2', alpha=0.2, edgecolor = None)
    ax2.legend(framealpha = 0.2, fontsize = 'small', ncol = 2)
    ax2.grid(True)
    
    pnlTime = time[[trade['out'] for trade in tradeList]]
    ax3.set_title('PNL', fontsize = 14)
    ax3.bar(pnlTime, pnlList, color = 'm', label = 'PNL_list')
    ax3.plot(pnlTime, pnlSum, color = 'r', label = 'PNL_sum')
    ax3.legend(framealpha = 0.2, fontsize = 'small', ncol = 2)
    ax3.grid(True)
    
    
    plt.setp(ax1.get_xticklabels()+ax2.get_xticklabels(), visible=False)
    plt.setp(ax3.get_xticklabels(), rotation = 30, horizontalalignment='right')

    fig.savefig(name)
    #os.startfile(name)
    plt.close(fig)

def plotSum(dataSet):
    fig = plt.figure(figsize = (20,10))
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212)
    pnlDF = pd.DataFrame()
    
    ax1.set_title('PNL_daily', fontsize = 14)
    for pairSet in dataSet:
        ax1.plot(pairSet[1].index, pairSet[1], label = pairSet[0])
        pnlDF = pd.concat([pnlDF, pd.DataFrame({'pnl':pairSet[1]})], axis = 1, join = 'outer')
    ax1.legend(framealpha = 0.2, fontsize = 'small', ncol = 9)
    ax1.grid(True)
    
    pnlDFSum = pnlDF.sum(axis = 1)
    ax2.set_title('PNL_sum', fontsize = 14)
    ax2.plot(pnlDFSum.index, pnlDFSum.cumsum())
    ax2.grid(True)
    
    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(ax2.get_xticklabels(), rotation = 30, horizontalalignment='right') # labels旋转角度        
    fig.savefig('PNL_all.png')
    plt.close(fig)

def printTrade(loadData, tradeList, pnlList, pnlSum, pnlDaily, pnlDailySum, f):
    print >>f, 'In_Time\t\tOut_Time\tLong\\Short\tPNL\t  PNL_sum\tInfo'
    time = loadData.index
    for i in xrange(len(tradeList)):
        trade = tradeList[i]
        text = time[trade['in']].strftime('%Y-%m-%d') + '\t' + time[trade['out']].strftime('%Y-%m-%d') + '\t'
        text += {1:loadData.columns[2] + '\\' + loadData.columns[0], 2:loadData.columns[0] + '\\' + loadData.columns[2]}[trade['short']]
        text += '\t%.4f\t  %.4f\t' % (pnlList[i], pnlSum[i])
        text += trade['msg']
        print >>f, text
    print >>f, '\nTime\t\t%s\t%s\tBasis1\tBasis2\tPNL_daily PNL_sum' % (loadData.columns[0], loadData.columns[2])
    for i in xrange(len(loadData)):
        print >>f,  '%s\t%d\t%d\t%d\t%d\t%.4f\t  %.4f' % (loadData.index[i].strftime('%Y-%m-%d'), loadData.ix[i,0], loadData.ix[i,2], loadData.ix[i,1], loadData.ix[i,3], pnlDaily[i], pnlDailySum[i])
    print >>f, '\n'

def trade(pair, cursor, listnum, f):
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
    
    setinf = lambda x : x if x != None else np.inf
    setminf = lambda x : x if x != None else -np.inf
    in_sign[0] = setinf(in_sign[0])
    in_sign[1] = setminf(in_sign[1])
    in_sign[2] = setinf(in_sign[2])
    in_sign[3] = setminf(in_sign[3])
    if out_sign != None:
        out_sign[0] = setminf(out_sign[0])
        out_sign[1] = setinf(out_sign[1])
        out_sign[2] = setminf(out_sign[2])
        out_sign[3] = setinf(out_sign[3])
    
    loadData1 = load(nameid1, start, end, coef1, cursor)
    loadData2 = load(nameid2, start, end, coef2, cursor)
    loadData = pd.concat([loadData1, loadData2], axis = 1, join = 'inner')
    if not len(loadData):
        print('No data returned when query %s_%s from %s to %s.' % (nameid1, nameid2, start, end))
        return False, None
    if loadData.index[0].strftime('%Y-%m-%d') != start or loadData.index[-1].strftime('%Y-%m-%d') != end:
        print('Data of pair %s_%s start from %s to %s' % (nameid1, nameid2, loadData.index[0].strftime('%Y-%m-%d'), loadData.index[-1].strftime('%Y-%m-%d')))
    # trade
    Basis1 = list(loadData[nameid1+'_b'])
    Basis2 = list(loadData[nameid2+'_b'])
    hold = False
    tradeList = []
    drawdown = 0
    for i in xrange(len(loadData)):
        if not hold:
             #有基差出入场信号
            if Basis1[i] >= in_sign[0] or Basis2[i] >= in_sign[2]: # 超过上阈值
                if Basis1[i]-in_sign[0] >= Basis2[i]-in_sign[2]:   # 1超过阈值更多
                    trigger = 1; short = 2                         # long 1
                else:                                              # 2超过阈值更多
                    trigger = 2; short = 1                         # long 2
                msg = '%s基差越上限入场，' % pair[trigger-1]
                in_time = i; hold = True; continue
            if Basis1[i] <= in_sign[1] or Basis2[i] <= in_sign[3]: # 超过下阈值
                if Basis1[i]-in_sign[1] >= Basis2[i]-in_sign[3]:   # 2超过阈值更多
                    trigger = 2; short = 2                         # short 2
                else:                                              # 1超过阈值更多
                    trigger = 1; short = 1                         # short 1
                msg = '%s基差越下限入场，' % pair[trigger-1]
                in_time = i; hold = True; continue
            
        
        #检查出场信号
        if hold and out_sign != None:
            if (trigger == 1 and short == 2 and Basis1[i] < out_sign[0]) or\
               (trigger == 1 and short == 1 and Basis1[i] > out_sign[1]) or\
               (trigger == 2 and short == 1 and Basis2[i] < out_sign[2]) or\
               (trigger == 2 and short == 2 and Basis2[i] > out_sign[3]):
                msg += '基差回归出场'
                tradeList.append({'in':in_time, 'out':i, 'short':short, 'msg':msg}); hold = False
                
        #检查持有时间
        if hold and tlim != None and i - in_time == tlim:
            msg += '达到最大持有时间出场'
            tradeList.append({'in':in_time, 'out':i, 'short':short, 'msg':msg}); hold = False
        #检查最大回撤
        if hold and dlim != None:
            drawdown = maxDrawdown(loadData[in_time:i], short, drawdown)
            if drawdown >= dlim/100.0:
                msg += '最大回撤达到%.2f%%出场' % drawdown*100
                tradeList.append({'in':in_time, 'out':i, 'short':short, 'msg':msg}); hold = False
    # 最后出场
    if hold:
        msg += '最后时间出场'
        tradeList.append({'in':in_time, 'out':len(loadData)-1, 'short':short, 'msg':msg}); hold = False
    # 计算PNL
    if not len(tradeList):
        print('No trading happens in pair %s_%s' % (nameid1, nameid2))
        return False, None
    pnlList, pnlDaily = calPNL(loadData, tradeList)
    pnlSum = np.cumsum(pnlList)
    pnlDailySum = pnlDaily.cumsum()
    printTrade(loadData, tradeList, pnlList, pnlSum, pnlDaily, pnlDailySum, f)
    name = '%d_%s_%s.png' % (listnum+1, loadData.columns[0], loadData.columns[2])
    plot(loadData, tradeList, pnlList, pnlSum, in_sign, name)
    return True, pnlDailySum


def backtest(watchlist):
    conn = sqlconn.connect(user='root', password='root', host='10.0.1.130')
    cursor = conn.cursor()
    filePath = os.path.dirname(os.path.realpath(__file__)) + '\\Report'
    if not os.path.isdir(filePath):
        os.mkdir(filePath)
    os.chdir(filePath)
    filePath += '\\' + datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    os.mkdir(filePath)
    os.chdir(filePath)
    f = open('report.txt','a')
    dataSet = []
    for listnum in xrange(len(watchlist)):
        pair = watchlist[listnum]
        print >>f, '====================== Pair %d ======================' % (listnum+1)
        success, pnlDailySum = trade(pair, cursor, listnum, f)
        if success:
            dataSet.append([str(listnum+1)+'_'+pair[0]+'_'+pair[1], pnlDailySum])
    f.close()
    cursor.close()
    conn.close()
    if not len(dataSet):
        print('The dataSet is empty!')
    else:
        plotSum(dataSet)
    os.startfile(filePath)


if __name__ == '__main__':
    watchlist = [\
        # nameid1, nameid2, coef1, coef2, start, end, in_sign(basis), out_sign(basis), time_limit, drawdown
        ['RB1310', 'RB1401', 1, 1, '2013-06-01', '2013-09-30', [-100, -200, None, None], [-150, -150, None, None], None, None],\
        ['RB1310', 'RB1401', 1, 1, '2013-10-01', '2013-12-30', [-100, -200, None, None], [None, None, None, None], 10, None],\
    ]
    backtest(watchlist)
