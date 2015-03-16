# -*- coding: utf-8 -*-
"""
Created on Fri Mar 06 11:32:16 2015

@author: Norris

"""

import os
import datetime
import matplotlib.pyplot as plt
import mysql.connector as sqlconn


try:
    conn = sqlconn.connect(user='root', password='root',host='127.0.0.1')
except sqlconn.Error as e:
    print('connect fails! {}'.format(e))

Time = []
Futures1 = []
Futures2 = []
Spot = []
Spread = []
Basis1 = []
Basis2 = []

cursor = conn.cursor()
query = 'select Time, Futures1, Futures2, Spot, Futures1-Futures2, \
Futures1-Spot, Futures2-Spot from futures.`ru1505-1509`'
cursor.execute(query)
for dbTime, dbFutures1, dbFutures2, dbSpot, dbSpread, dbBasis1, dbBasis2 in cursor:
    Time.append(dbTime)
    Futures1.append(dbFutures1)
    Futures2.append(dbFutures2)
    Spot.append(dbSpot)
    Spread.append(dbSpread)
    Basis1.append(dbBasis1)
    Basis2.append(dbBasis2)
conn.close()

now = datetime.datetime.now()
time = now.strftime('%Y-%m-%d %H-%M-%S')
FilePath = 'D:\\FuturesWebApp\\Output\\' + time

if not os.path.exists(FilePath):
    os.makedirs(FilePath)
os.chdir(FilePath)
text = 'Futures1 = 1.0 * RB1505.SHF    Futures2 = 1 * RB1510.SHF    From 2014-09-16 to 2015-02-27'


fig = plt.figure(figsize = (10,8))

ax1 = fig.add_subplot(211)
ax2 = fig.add_subplot(212, sharex = ax1)

#ax1.plot(Time, Futures1, 'c', label = 'Futures1')
ax1.plot(Time, Futures1, color = '#0000FF', label = 'Futures1')
ax1.plot(Time, Futures2, color = '#00FFFF', label = 'Futures2')
ax1.plot(Time, Spot, 'r:', label = 'Spot')

ax1.set_title('Prices', fontsize = 20)
ax1.grid(True)
ax1.legend(framealpha = 0.2, fontsize = 'small', ncol =  3)

ax2.set_title('Spread', fontsize = 20)
ax2.plot(Time, Spread, 'c', label = 'Spread')
ax2.plot(Time, Basis1, 'r:', label = 'Basis1')
ax2.plot(Time, Basis2, 'b', label = 'Basis2')
ax2.legend(framealpha = 0.2, fontsize = 'small', ncol =  3)
#ax2.axhline(y = 500, linewidth = 1, color='g', linestyle = '--')
ax2.axhspan(200, 300, facecolor='0.5', alpha=0.5, edgecolor  = None)
ax2.grid(True)


#xticklabels = ax1.get_xticklabels()
#plt.setp(xticklabels, visible=False)
fig.text(0.02, 0.95, text, bbox=dict(facecolor='grey', alpha=0.2), fontsize = 10)
fig.savefig('test.png')

plt.setp(ax1.get_xticklabels(), visible=False)
plt.setp(ax2.get_xticklabels(), rotation = 30, horizontalalignment='right') # labels旋转角度       



