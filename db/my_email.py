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
    name = ConfigTagTemp['nameid1'] + '_' + ConfigTagTemp['nameid2'] + '.png'
    print('Add image into email: ' + name + ' ' + str(imageId))
    fp = open(name, 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()
    msgImage.add_header('Content-ID', '<%dimage>' % imageId)
    return msgImage
    
    
    
def email(Config, myEmail):
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
        ConfigTag = Config[Config['nameTag'] == nameTagList[i]].sort(columns = ['nameid1','nameid2'])
        nImage = len(ConfigTag)
        text += '<hr style="border:1px dashed #F00" width="90%" size="1" /><br>'
        text += '<font face="Times New Roman" size="6"><b>%s</b></font><br>' % nameTagList[i]
        while nImage >= 2:
            text += '<img src="cid:%dimage"><img src="cid:%dimage"><br>' % (imageId, imageId + 1)
            msgRoot.attach(addImage(imageId, ConfigTag.iloc[len(ConfigTag)-nImage]))
            msgRoot.attach(addImage(imageId+1, ConfigTag.iloc[len(ConfigTag)-nImage+1]))
            nImage -= 2
            imageId += 2
        if nImage == 1:
            text += '<img src="cid:%dimage"><br>' % imageId
            msgRoot.attach(addImage(imageId, ConfigTag.iloc[len(ConfigTag)-nImage]))
            imageId += 1
            

    
    msgText = MIMEText(text,'html','utf-8')
    msgRoot.attach(msgText)

    smtp = smtplib.SMTP()  
    smtp.connect(myEmail['smtpserver'])  
    #smtp.login(myEmail['sender'], myEmail['password']) # when no password
    smtp.sendmail(myEmail['sender'], myEmail['receiver'], msgRoot.as_string())
    smtp.quit()
    return True, None