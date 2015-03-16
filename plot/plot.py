# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 16:48:41 2015

@author: norris
"""

# 读取Config
# 连接本地数据库，拉取数据
# 输出图像


import datetime
import os
import pandas as pd
import mysql.connector as sqlconn
import matplotlib.pyplot as plt

import update
import my_email

def plotDb(Config, f):
    if len(Config.index) == 0:
        return False, 'config.csv is emply!'
    for i in xrange(len(Config.index)):
        name = Config.ix[i,:]
        # Connect to db
        try:
            conn = sqlconn.connect(user='root', password='root',host='127.0.0.1')
            cursor = conn.cursor()
        except sqlconn.Error as e:
            return False, 'Connect fails! {}'.format(e)
        
        Time = []
        Futures1 = []
        Futures2 = []
        Spread = []
        #Spot = []
        #Basis1 = []
        #Basis2 = []
        
        try:
            query = 'SELECT a.Time Time, a.Close Futures1, b.Close Futures2 FROM \
            (SELECT Close, Time FROM futures.prices WHERE Symbol = \'%s\') a JOIN \
            (SELECT Close, Time FROM futures.prices WHERE Symbol = \'%s\') b on \
            a.Time = b.Time' % (name['nameid1'], name['nameid2'])
            cursor.execute(query)
        except sqlconn.Error as e:
            cursor.close()
            conn.close()
            return False, query + '\nQuery fails! {}'.format(e)
        for dbTime, dbFutures1, dbFutures2 in cursor:
            Time.append(dbTime)
            Futures1.append(dbFutures1*name['coef1'])
            Futures2.append(dbFutures2*name['coef2'])
            Spread.append(Futures1[-1]-Futures2[-1])
            #Spot.append(dbSpot)
            #Basis1.append(dbBasis1)
            #Basis2.append(dbBasis2)
        print('Plotting ' + name['nameid1'] + '_' + name['nameid2'] + '.png')
        print >>f, 'Plotting ' + name['nameid1'] + '_' + name['nameid2'] + '.png'
        text = 'Time: ' + Time[0].strftime('%Y-%m-%d') + ' to ' + Time[-1].strftime('%Y-%m-%d')

        fig = plt.figure(figsize = (10,8))
        ax1 = fig.add_subplot(211)
        ax2 = fig.add_subplot(212)
        ax1.set_title('Price', fontsize = 20)
        ax1.plot(Time, Futures1, color = '#0000FF', label = name['nameid1'])
        ax1.plot(Time, Futures2, color = '#00FFFF', label = name['nameid2'])
        #ax1.plot(Time, Spot, 'b', label = 'Spot')
        ax1.legend(framealpha = 0.2, fontsize = 'small', ncol =  2) # ncol = 3
        ax1.grid(True)
        
        ax2.set_title('Spread', fontsize = 20)
        ax2.plot(Time, Spread, 'c', label = 'Spread')
        #ax2.plot(Time, Basis1, 'r', label = 'Basis1')
        #ax2.plot(Time, Basis2, 'b', label = 'Basis2')
        ax2.axhspan(name['lowerlim'], name['upperlim'], facecolor='0.3', alpha=0.3, edgecolor  = None)
        ax2.legend(framealpha = 0.2, fontsize = 'small')
        ax2.grid(True)
        
        fig.text(0.02, 0.95, text, bbox=dict(facecolor='grey', alpha=0.2), fontsize = 10)
        fig.savefig(name['nameid1'] + '_' + name['nameid2'] + '.png')
        
    cursor.close()
    conn.close()
    return True, None

def plot(FTPPath, confFile, myEmail):
    f = open('update.log','a')
    Config = pd.read_csv(confFile)
    print('Plot begins at %s' % datetime.datetime.now())
    print >>f, 'Plot begins at %s' % datetime.datetime.now()
    try:
        FilePath = FTPPath + '\\' + datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        os.makedirs(FilePath)
        os.chdir(FilePath)
        success, msg = plotDb(Config, f) # main plot func
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
        print('Plot complete')
        print >>f, 'Plot complete'
        
    # Sending email
    if myEmail['mark']:
        print('Email begins at %s' % datetime.datetime.now())
        print >>f, 'Email begins at %s' % datetime.datetime.now()
        os.chdir(FTPPath)
        if not os.path.isfile(myEmail['emailFile']):
            print >>f, 'File %s does not exist!' % myEmail['emailFile']
            f.close()
            return False
        emailDF = pd.read_csv(myEmail['emailFile'], dtype = str) # get email config from email.csv
        myEmail['receiver'] = list(emailDF['receiver'])
        myEmail['sender'] = emailDF['sender'][0]
        myEmail['password'] = emailDF['password'][0]
        myEmail['smtpserver'] = emailDF['smtpserver'][0]
        os.chdir(FilePath)
        try:
            success, msg = my_email.email(Config, myEmail) # main email func
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
            print('Email complete')
            print >>f, 'Email complete'
    f.close()
    return True




if __name__ == '__main__':
    FTPPath = r'D:\FTP'
    confFile = 'config.csv'
    myEmail = {'mark':False,'emailFile':'email.csv'} # email settings
    
    os.chdir(FTPPath)
    if update.update(confFile):
        plot(FTPPath, confFile, myEmail)

        
    os.startfile(FTPPath)


