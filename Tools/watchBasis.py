# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 09:34:36 2015

@author: norris
"""


import datetime
import mysql.connector as sqlconn
import matplotlib.pyplot as plt
import numpy as np
import os

# 取出前两位字符中的字母
# name = 'CU1505.DCE' -> return 'CU'
def getNameid(name):
    return "".join([a for a in name[:2] if a.isalpha()])

# 添加后缀标签
# name = 'CU1505' -> return 'CU1505.SHF'
def addSuffix(name):
    nameid = getNameid(name)
    if nameid in {'CU','AL','ZN','PB','AU','AG','RB','RU','FU','WR','BU','HC','IM'}:
        return name + '.SHF'
    if nameid in {'A','M','Y','P','C','I','JM','J','L','V','B','JD','FB','BB','PP','CS'}:
        return name + '.DCE'
    return name + '.CZC'

# 读取数据库数据
# eg. pair = ['C1205', '2011-03-01','2012-05-10'] name = 'C1205.DCE' spotid = 'S0182161'
def load(pair, cursor):
    name = addSuffix(pair[0])
    query = "SELECT spotid FROM futures.spotlist WHERE nameid = '%s' LIMIT 1" % getNameid(name)
    cursor.execute(query)
    for id in cursor:
        spotid = id[0] # 取现货标签
    start = pair[1] # 开始时间
    if len(pair) > 2:
        end = pair[2]
    else:
        end = datetime.datetime.now().strftime('%Y-%m-%d')
    Time = []
    Basis = []
    query = "SELECT * FROM \
    (SELECT a.Time Time, a.Close Futures, b.Close Spot FROM \
    (SELECT Close, Time FROM futures.prices WHERE Symbol = '%s') a JOIN \
    (SELECT Close, Time FROM futures.prices WHERE Symbol = '%s') b on \
    a.Time = b.Time) c WHERE Time BETWEEN \
    DATE_FORMAT('%s','%%Y-%%m-%%d') AND DATE_FORMAT('%s','%%Y-%%m-%%d')"\
    % (name, spotid, start, end)
    cursor.execute(query)
    # 执行查询后依次取出数据
    for dbTime, dbFutures, dbSpot in cursor:
        Time.append(dbTime)
        Basis.append(dbSpot-dbFutures)
    if not len(Time):
        print('No data returned when query %s from %s to %s.' % (pair[0], start, end))
        return False, None
    return True, [pair[0], Time, Basis]


def plot(dataSet):
    
    dataAll = [] # 保存所有价格数据，用于统计
    # 建立fig，设置各图片位置及大小
    fig = plt.figure(figsize = (24,12))
    rect_ax = [0.05, 0.1, 0.75, 0.85]
    rect_axHist = [0.83, 0.1, 0.10, 0.85]
    ax = fig.add_subplot(121, position = rect_ax)
    axHist = fig.add_subplot(122, position = rect_axHist)
    
    # 画图，设置标题、图例、网格
    ax.set_title('Basis', fontsize = 14)
    for pairSet in dataSet:
        ax.plot(pairSet[1], pairSet[2], label = pairSet[0])
        dataAll += pairSet[2]
    ax.legend(framealpha = 0.2, fontsize = 'small', ncol = 9)
    ax.grid(True)
    
    # 右侧直方图
    axHist.hist(dataAll, bins = 20, facecolor='#0000FF', alpha=0.5, orientation='horizontal')
    axHist.grid(True)
    
    # 设置labels旋转角度    
    plt.setp(ax.get_xticklabels(), rotation = 30, horizontalalignment='right')     
    dataAllNP = np.array(dataAll)
    
    # 添加右下角统计量文本text
    text = 'Obs: %d\nMax: %.2f\nMin: %.2f\nMean: %.2f\nStd: %.2f\nMedian: %.2f\n' \
    % (len(dataAll), dataAllNP.max(), dataAllNP.min(), dataAllNP.mean(), dataAllNP.std(), np.median(dataAllNP))
    ptl = np.percentile(dataAllNP, [90,80,70,60,50,40,30,20,10], interpolation = 'higher')
    for i in xrange(1,10):
        text += '%d%%: %.2f\n' % (100-i*10, ptl[i-1])
    fig.text(0.94, 0.1, text, bbox=dict(facecolor='grey', alpha=0.2), fontsize = 10)
    fig.savefig('test.png')
    os.startfile('test.png')
    plt.close(fig)
    
    
def watchBasis(watchlist):
    # 连接到数据库 host=固定IP cursor为query的接口
    conn = sqlconn.connect(user='root', password='root', host='10.0.1.130')
    cursor = conn.cursor()
    
    # 读取每一个pair的数据，并全部存储在dataSet中，对dataSet绘图
    # dataSet=[data1, data2, ...]
    dataSet = []
    for pair in watchlist:
        success, data = load(pair, cursor)
        if success:
            dataSet.append(data)
    cursor.close()
    conn.close()
    if not len(dataSet):
        print('The dataSet is empty!')
    else:
        plot(dataSet)
    

if __name__ == '__main__':
    watchlist = [\
        ['C1205', '2011-03-01','2012-05-10']
    ]
    watchBasis(watchlist)
