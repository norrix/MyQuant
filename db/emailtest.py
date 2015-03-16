# -*- coding: utf-8 -*-
"""
Created on Fri Mar 13 15:05:41 2015

@author: norris
"""

import datetime
import os

import pandas as pd
import smtplib  
from email.mime.multipart import MIMEMultipart  
from email.mime.text import MIMEText  
from email.mime.image import MIMEImage


FTPPath = r'D:\FTP'
FilePath = r'D:\FTP\2015-03-13 14-07-50'
myEmail = {'mark':False,'emailFile':'email.csv'}
nameList = ['CU1505.SHF_CU1509.SHF.png']

os.chdir(FTPPath)

emailDF = pd.read_csv(myEmail['emailFile'], dtype = str) # get email config from email.csv
myEmail['receiver'] = list(emailDF['receiver'])
myEmail['sender'] = emailDF['sender'][0]
myEmail['password'] = emailDF['password'][0]
myEmail['smtpserver'] = emailDF['smtpserver'][0]
os.chdir(FilePath)

print('Email begins at %s' % datetime.datetime.now())
msgRoot = MIMEMultipart('related')
msgRoot['Subject'] = u'期货价格日报 ' + datetime.datetime.now().strftime('%Y-%m-%d')

msgText = MIMEText('<b>Some <i>HTML</i> text</b> and an image.<br><img src="cid:image1"><br>good!','html','utf-8')
msgRoot.attach(msgText)  
  
fp = open(FilePath + '\\' + nameList[0], 'rb')
msgImage = MIMEImage(fp.read())
fp.close()
msgImage.add_header('Content-ID', '<image1>')
msgRoot.attach(msgImage)
smtp = smtplib.SMTP()  
smtp.connect(myEmail['smtpserver'])  
#smtp.login(myEmail['sender'], myEmail['password'])
smtp.sendmail(myEmail['sender'], myEmail['receiver'], msgRoot.as_string())
smtp.quit()