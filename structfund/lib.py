# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

def isDateEarlier(date1, date2):
    date1 = datetime.strptime(date1, '%Y-%m-%d') if type(date1) == str else date1
    date2 = datetime.strptime(date2, '%Y-%m-%d') if type(date2) == str else date2
    if date1 < date2:
        return True
    else:
        return False

def isWorkday(date):
    date = datetime.strptime(date, '%Y-%m-%d') if type(date) == str else date
    if date.isoweekday() in (1, 2, 3, 4, 5):
        return True
    else:
        return False
        
def nextWorkday(date):
    date = datetime.strptime(date, '%Y-%m-%d') if type(date) == str else date
    weekday = date.isoweekday()
    if weekday in (1, 2, 3, 4, 7):
        return (date + timedelta(1)).strftime('%Y-%m-%d')
    elif weekday == 5:
        return (date + timedelta(3)).strftime('%Y-%m-%d')
    else: # weekday == 6
        return (date + timedelta(2)).strftime('%Y-%m-%d')

def previousWorkday(date):
    date = datetime.strptime(date, '%Y-%m-%d') if type(date) == str else date
    weekday = date.isoweekday()
    if weekday in (2, 3, 4, 5, 6):
        return (date - timedelta(1)).strftime('%Y-%m-%d')
    elif weekday == 1:
        return (date - timedelta(3)).strftime('%Y-%m-%d')
    else: # weekday == 7
        return (date - timedelta(2)).strftime('%Y-%m-%d')