# -*- coding: utf-8 -*-
"""
Created on Fri Mar 13 18:11:29 2015

@author: norris
"""

import re
import datetime
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


def addImage(imageId, ConfigTagTemp):
    # name = 'CU1505.SHF_CU1509.SHF.png'
    fp = open(ConfigTagTemp['nameid1'] + '_' + ConfigTagTemp['nameid2'] + '.png', 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()
    msgImage.add_header('Content-ID', '<image%d>' % imageId)
    return msgImage
    
    
    
def email(Config, myEmail):
    print('Email begins at %s' % datetime.datetime.now())
    #return True, None
    #生成品种标签
    
    if not len(Config):
        return False, 'Error! Config is empty'
    
    nameTag = []
    pattern = re.compile('^[A-Z]+')
    for i in xrange(len(Config)):
        match1 = pattern.search(Config.ix[i]['nameid1'])
        match2 = pattern.search(Config.ix[i]['nameid2'])
        if match1 and match2:
            nameTag.append(match1.group() + '_' + match2.group())
        else:
            return False, 'Error! Find nameTag from \'' + \
            Config.ix[i]['nameid1'] + '\' and \'' + Config.ix[i]['nameid2'] + '\''
    nameTag = pd.DataFrame(nameTag, columns = ['nameTag'])
    Config = pd.concat([Config, nameTag], axis = 1).sort(columns = 'nameTag')
    nameTagList = sorted(list(set(nameTag['nameTag'])))

    
    
    # msgRoot
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = u'期货价格日报 ' + datetime.datetime.now().strftime('%Y-%m-%d')
    
    text = ''
    imageId = 0
    for i in xrange(len(nameTagList)):
        ConfigTag = Config[Config['nameTag'] == nameTagList[i]]
        nImage = len(ConfigTag)
        text += 'nameTag: <b>%s</b><br>' % nameTagList[i]
        while nImage >= 2:
            text += '<img src="cid:image%d"><img src="cid:image%d"><br>' % (imageId, imageId + 1)
            msgRoot.attach(addImage(imageId, ConfigTag.iloc[imageId]))
            msgRoot.attach(addImage(imageId+1, ConfigTag.iloc[imageId+1]))
            nImage -= 2
            imageId += 2
        if nImage == 1:
            text += '<img src="cid:image%d"><br>' % imageId
            msgRoot.attach(addImage(imageId, ConfigTag.iloc[imageId]))
            

    # get email config from email.csv
    emailDF = pd.read_csv(myEmail['emailFile'], dtype = str) 
    myEmail['receiver'] = list(emailDF['receiver'])
    myEmail['sender'] = emailDF['sender'][0]
    myEmail['password'] = emailDF['password'][0]
    myEmail['smtpserver'] = emailDF['smtpserver'][0]
    msgText = MIMEText(text,'html','utf-8')
    msgRoot.attach(msgText)

    smtp = smtplib.SMTP()  
    smtp.connect(myEmail['smtpserver'])  
    #smtp.login(myEmail['sender'], myEmail['password']) # when no password
    smtp.sendmail(myEmail['sender'], myEmail['receiver'], msgRoot.as_string())
    smtp.quit()

