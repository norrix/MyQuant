# -*- coding: utf-8 -*-
"""
Created on Mon Mar 09 14:44:30 2015

@author: norris

"""
# Update Database from Wind
# futures.`prices`: name, time, price, vol
import os
import datetime
import math
import pandas as pd
import mysql.connector as sqlconn
from WindPy import w


# insert update time into db after data updated
def updateTime(cursor, conn):
    try:
        query = 'INSERT INTO futures.`update` (Time) VALUES (\'%s\')' % datetime.datetime.now()
        cursor.execute(query)
        conn.commit()
    except sqlconn.Error as e:
        cursor.close()
        conn.close()
        return False, query + '\nQuery fails! {}'.format(e)
    cursor.close()
    conn.close()
    return True, None


def updateDb(Config, f):
    now = datetime.datetime.now()
    todayValid = now.time() > datetime.time(15, 30) #15:30之后update到今日数据，否则update到昨天


    # Connect to wind
    wind_re = w.start()
    if wind_re.ErrorCode != 0:
        return False, 'Failed to connect wind!'
        
    # Connect to MySql
    try:
        conn = sqlconn.connect(user='root', password='root', host='127.0.0.1')
        cursor = conn.cursor()
    except sqlconn.Error as e:
        return False, 'Connect fails! {}'.format(e)

    # Get newNameList and oldNameList
    newNameList = set(list(Config['nameid1']) + list(Config['nameid2']))
    try:
        query = 'SELECT DISTINCT Symbol FROM futures.`prices`'
        cursor.execute(query)
    except sqlconn.Error as e:
        return False, query + '\nQuery fails! {}'.format(e)
    oldNameList = set()
    for Symbol in cursor:
        oldNameList.add(Symbol[0].encode('utf-8'))
  
    
    #oldNameList中不存在的新商品，需要重新从头更新，更新到today/yestoday
    for name in newNameList - oldNameList:
        ipo_date = w.wsd(name, 'ipo_date').Data[0][0]  # get ipo_date
        if ipo_date == None:
            w.close()
            cursor.close()
            conn.close()
            return False, 'WSD fails! IPO_DATE of \"' + name + '\" does not exist!'
        beginTime = (ipo_date + datetime.timedelta(1)).strftime('%Y-%m-%d')
        endTime = (now - (not todayValid) * datetime.timedelta(1)).strftime("%Y-%m-%d")
        wind_re = w.wsd(name, 'open,high,low,close,volume,oi', beginTime, endTime, "Fill=Previous") # get raw data
        if wind_re.ErrorCode != 0:
            w.close()
            cursor.close()
            conn.close()
            return False, 'WSD fails! Name = \'%s\' beginTime = \'%s\' endTime = \'%s\' ErrorCode = %d Info = \'%s\'' % \
            (name, beginTime, endTime,wind_re.ErrorCode, wind_re.Data[0][0])
        print('INSERT %s INTO DATABASE... (%s to %s)' % (name, beginTime, endTime))
        print >>f, 'INSERT %s INTO DATABASE... (%s to %s)' % (name, beginTime, endTime)
        try:
            addCount = 0
            query = 'REPLACE INTO futures.`prices` VALUES ' # insert data into db
            for i in xrange(len(wind_re.Times)):
                if not math.isnan(wind_re.Data[0][i]):
                    addCount += 1
                    query += '(\'%s\', \'%s\', %f, %f, %f, %f, %f, %f)' % \
                    (name, wind_re.Times[i].strftime('%Y-%m-%d'), wind_re.Data[0][i], wind_re.Data[1][i], \
                    wind_re.Data[2][i], wind_re.Data[3][i], wind_re.Data[4][i], wind_re.Data[5][i])
                    if i < len(wind_re.Times)-1:
                        query += ','
            if not addCount == 0:
                cursor.execute(query)
                conn.commit()
        except sqlconn.Error as e:
            w.close()
            cursor.close()
            conn.close()
            return False, query[:300] + '...\nQuery fails! {}'.format(e)
    
    #oldNameList中的商品从上一次成功更新时间开始更新，更新到today/yestoday
    #如果是第一次更新，即update为空表，则更新已完成
    try:
        query = 'SELECT time FROM futures.`update` ORDER BY updateid DESC LIMIT 1' # get last update time
        cursor.execute(query)
    except sqlconn.Error as e:
        w.close()
        cursor.close()
        conn.close()
        return False, 'Query fails! {}'.format(e)
    time = None
    time = cursor.fetchone()
    if time == None:
        # 第一次更新
        w.close()
        return updateTime(cursor, conn)
    else:
        time = time[0]
    if (now - time).days == 0: #上一次更新为今天
        if time.time() < datetime.time(15, 30) and todayValid:
            beginTime, endTime = time, time
        else:
            w.close()
            return updateTime(cursor, conn)
    else: #上一次更新不是今天
        beginTime = time + (time.time() > datetime.time(15, 30)) * datetime.timedelta(1)
        endTime = now - (not todayValid) * datetime.timedelta(1)
    name = ','.join(oldNameList)
    while (beginTime - endTime).days <= 0: # every tradeDate from beginTime to endTime
        tradeDate = beginTime.strftime('%Y-%m-%d')
        wind_re = w.wss(name, 'open,high,low,close,volume,oi', 'tradeDate=%s;priceAdj=F;cycle=D' % tradeDate) # get raw data
        if wind_re.ErrorCode != 0:
            w.close()
            cursor.close()
            conn.close()
            return False, 'WSS fails! Name = \'%s\' tradeDate = \'%s\' ErrorCode = %d Info = \'%s\'' % \
            (name, tradeDate, wind_re.ErrorCode, wind_re.Data[0][0])
        print('INSERT %s INTO DATABASE... (%s)' % (name, tradeDate))
        print >>f, 'INSERT %s INTO DATABASE... (%s)' % (name, tradeDate)
        
        try:
            addCount = 0
            query = 'REPLACE INTO futures.`prices` VALUES ' # insert data into db
            for i in xrange(len(wind_re.Codes)):
                if not math.isnan(wind_re.Data[0][i]):
                    addCount += 1
                    query += '(\'%s\', \'%s\', %f, %f, %f, %f, %f, %f)' % \
                    (wind_re.Codes[i], wind_re.Times[0].strftime('%Y-%m-%d'), wind_re.Data[0][i], wind_re.Data[1][i], \
                    wind_re.Data[2][i], wind_re.Data[3][i], wind_re.Data[4][i], wind_re.Data[5][i])
                    if i < len(wind_re.Codes)-1:
                        query += ','
            if not addCount == 0:
                cursor.execute(query)
                conn.commit()
        except sqlconn.Error as e:
            w.close()
            cursor.close()
            conn.close()
            return False, query[:300] + '...\nQuery fails! {}'.format(e)
        beginTime += datetime.timedelta(1)
    w.close()
    return updateTime(cursor, conn)
    

def update(confFile):
    #获取config.csv商品列表newNameList，获取db商品列表oldNameList
    f = open('update.log','a')
    if not os.path.isfile(confFile):
        print >>f, 'File %s does not exist!' % confFile
        f.close()
        return False
    Config = pd.read_csv(confFile)
    print('Update begins at %s' % datetime.datetime.now())
    print >>f, '========================================'
    print >>f, 'Update begins at %s' % datetime.datetime.now()
    try:
        success, msg = updateDb(Config, f)
    except Exception:
        import traceback
        traceback.print_exc(file = f)
        f.close()
        return False
        
    if not success:
        print('An error occured at %s' % datetime.datetime.now())
        print >>f, 'An error occured at %s' % datetime.datetime.now()
        print >>f, msg
        f.close()
        return False
    else:
        print('Complete')
        print >>f, 'Update database complete'
        f.close()
        return True
    
if __name__ == '__main__':
    FTPPath = r'D:\FTP'
    confFile = 'config.csv'
    os.chdir(FTPPath)
    update(confFile)
    os.startfile(FTPPath)
    
