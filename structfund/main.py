# -*- coding: utf-8 -*-
"""
Created on Tue Mar  1 15:04:36 2016

@author: norrix
"""

# cmd
# sys.setdefaultencoding('utf8')
from structfund import StructFund
from datetime import datetime
import os
fundList = [('160417.OF', '华安沪深300', 'HS300A', 'HS300B'), 
            ('161507.OF', '银河沪深300成长', '银河优先', '银河进取'),
            ('161718.OF', '招商沪深300高贝塔', '高贝塔A', '高贝塔B'),
            ('161811.OF', '银华沪深300分级', '银华300A', '银华300B'),
            ('165515.OF', '信诚沪深300分级', '沪深300A', '沪深300B'),
            ('166802.OF', '浙商沪深300', '国金300A', '国金300B'),
            ]
weightList = {'华安沪深300':1,}

if __name__ == '__main__':
    result_dir = 'backtest_%s' % datetime.now().strftime('%Y%m%d_%H%M%S')
    os.mkdir(result_dir)
    os.chdir(result_dir)
    tradeList = []
    for fund_info in fundList:
        fund = StructFund(fund_info)
        fund.connect_db('../funds.db')
        fund.backtest(startdate = '2013-01-01', enddate = '2016-01-01', premium = True, discount = True)
        tradeList = tradeList + fund.tradeList # {'time':time,'rtn':rtn,'type':trade[0],'target':self.fundname}
        del fund
        
    tradeList.sort(key=lambda x:x['time'])
    f = open('backtest.txt', 'w+')
    f.write('时间\t\t\t\t\t\t收益\t\t累计收益\t\t类型\t\t标的\n')
    netvalue = 1
    for trade in tradeList:
        netvalue = netvalue * (1 + trade['rtn'])
        f.write('%s\t\t%.2f%%\t%.2f%%\t\t%s\t\t%s\n' % (trade['time'].strftime('%Y-%m-%d %H:%M:%S'), trade['rtn']*100, (netvalue-1)*100, {1:'溢价', 2:'折价'}[trade['type']], trade['target']))
    f.close()
    
    