# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 10:07:29 2021

@author: 好鱼
"""

import matplotlib.pyplot as plt
import math

import os
import sys

import smtplib
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.header import Header
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.utils import parseaddr, formataddr



def send_mails(path, files, pics, receiver, str_subject, str_content):
    ''' 通过邮件发送数据 '''

    #构造邮件对象
    msg = MIMEMultipart()
    
    # 发送者的登陆用户名和密码
    user = 'wangdu14008@163.com'
    password = '200514008'
    password = 'QLKVCLWOAZNLRMFC'
    smtpserver = 'smtp.163.com' #发送者邮箱的SMTP服务器地址
    
    sender = user
    msg['From'] = Header(sender, 'ascii')
    msg['To'] =','.join(receiver)
    
    #添加标题、正文、附件和图片
    msg['Subject'] = Header(str_subject, 'utf-8').encode()  # 添加邮件标题
    
    # 添加邮件文本和格式
    html_info = '<html><head><style>#string{text-align:left;font-size:16px;}</style><div id="string">' + str_content + '<div></head><body>'
    for u in pics:
        html_info = html_info + '<img src="cid:'+u+'" alt="'+u+'">'
    html_info = html_info + '<body></html>'
    content = MIMEText(html_info, 'html','utf-8')
    msg.attach(content)                                    # 添加邮件正文

    # 添加邮件附件
    for u in files:
        att = MIMEText(open(path+u, 'rb').read(), 'base64', 'utf-8')
        # att["Content-Type"] = 'application/octet-stream'
        att["Content-Disposition"] = 'attachment; filename='+u
        msg.attach(att)
        
    # 添加邮件图片
    for u in pics:
        att = MIMEImage(open(path+u, 'rb').read())
        att.add_header("Content-ID", u)
        msg.attach(att)

    
    smtp = smtplib.SMTP() #实例化SMTP对象
    smtp.connect(smtpserver,25) #（缺省）默认端口是25 也可以根据服务器进行设定
    smtp.login(user,password) #登陆smtp服务器
    smtp.sendmail(sender,msg['To'].split(','),msg.as_string()) #发送邮件，这里有三个参数
    smtp.quit()
    print(">>> 邮件发送成功")



if __name__=='__main__':
    
    # path = "C:/Investment/TradeOrders/"
    # list_files = ["ETFtiming_orders.csv", "NorthBoundFollow20210325.csv"]
    # list_pics = ["Figure2020.png", "Figure2021.png"]
    # list_receiver = ['wangdu14008@qq.com', 'wangdu2639@dingtalk.com', 'wangdu@jiguang.cn']
    # send_mails(path, list_files, list_pics, list_receiver, "Mail Subject", "Mail Content")
    pass
