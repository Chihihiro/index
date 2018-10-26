import os
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
from email.mime.multipart import MIMEMultipart


import smtplib

def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))

# 输入Email地址和口令:
from_addr = '632207812@qq.com'
password = 'aphstjyszatqbecc'
# 输入收件人地址:
to_addr = '632207812@qq.com'
# 输入SMTP服务器地址:
smtp_server = 'smtp.qq.com'
msg = MIMEMultipart()
msg.attach(MIMEText('pdf解析完成请查收', 'plain', 'utf-8'))
# msg = MIMEText('hello, send by Python...', 'plain', 'utf-8')#正文
msg['From'] = _format_addr('秦晓东 <%s>' % from_addr) #发件人 不该就成为邮箱地址
msg['To'] = _format_addr('收件人 <%s>' % to_addr) #收件人
msg['Subject'] = Header('pdf解析的报告', 'utf-8').encode()# 标题

path = os.getcwd()


att1 = MIMEText(open('C:/Users/qinxd/Desktop/nibao.xlsx', 'rb').read(), 'base64', 'utf-8')
att1["Content-Type"] = 'application/octet-stream'
att1["Content-Disposition"] = 'attachment; filename="nibao.xlsx"'
msg.attach(att1)

server = smtplib.SMTP(smtp_server, 25)
server.set_debuglevel(1)
server.login(from_addr, password)
server.sendmail(from_addr, [to_addr], msg.as_string())
server.quit()

