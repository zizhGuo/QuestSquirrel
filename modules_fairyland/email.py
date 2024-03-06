import os
import pandas as pd
CUR_PATH = os.path.dirname(os.path.abspath(__file__))
print(os.path.abspath(__file__))
print(os.path.dirname(os.path.abspath(__file__)))

from smtplib import SMTP_SSL
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.header import Header
from email.utils import formataddr
from email import encoders
from smtplib import SMTP_SSL
import base64

# python type placeholder
from typing import List

class Email:
    # def __init__(self, message, subject, header_from, header_to, sender_email, recipient_show, cc_show, user, password, to_addrs, email_company, attch_root_dir, output_file_name):
    def __init__(self, config: dict, root_path):
        # self.email_config = config
        self.message = config['email']['message']
        self.subject = config['email']['subject']
        self.header_from = config['email']['header_from']
        self.sender_email = config['email']['sender_email']
        self.header_to = config['email']['header_to']
        self.recipient_show = config['email']['recipient_show']
        self.cc_show = config['email']['cc_show']
        self.user = config['email']['user']
        self.password = config['email']['password']
        self.to_addrs = config['email']['to_addrs']
        self.email_company = config['email']['email_company']
        
        # init report result directory
        self.temp_save_path = config['connector']['temp_save_path']
        self.read_file = config['email']['read_file']
        self.read_file_format = config['email']['read_file_format']

        self.root_path = root_path
        self.end_dt = config['end_dt']

        # init email sending report name

        # dt = '20231213'
        self.send_file = config['email']['send_file']
        # self.send_file_name = '{}_{}.xlsx'.format(self.send_file_name, dt)
        self.send_file_format = config['email']['send_file_format']
        # self.send_visual_name = '{}_{}.html'.format(self.send_visual_name, dt)
    
    def _get_attachment_path(self):

        dir_path = os.path.join(self.root_path, self.temp_save_path, self.end_dt)
        # file_path = os.path.join(dir_path, self.output_dir)
        self.attachments = []
        for i in range(len(self.read_file)):
            try:
                file_read = os.path.join(dir_path, self.read_file[i])+'.'+self.read_file_format[i]
                if not os.path.exists(file_read):
                    print(f"file {self.read_file[i]} not existed")
                    continue
                file_write = os.path.join(dir_path, self.send_file[i])+f'-{self.end_dt}'+'.'+self.send_file_format[i]
                os.system('cp \'{}\' \'{}\''.format(file_read, file_write))
                self.attachments.append(file_write)
            except Exception as e:
                print('attachment copy failed')
                print(e)
                return
        print('All attachments copy succeeded')

        # self.report = os.path.join(self.root_path, self.report_save_path)+'/'+self.read_file_name+'_'+self.date+'.xlsx'
        # # df = pd.read_excel(self.report)
        # # copy report file at same directory and rename the copy
        # self.new_report = os.path.join(self.root_path, self.report_save_path)+'/'+self.send_file_name+'_'+self.date+'.xlsx'
        # os.system('cp {} {}'.format(self.report, self.new_report))
        # self.new_visual = os.path.join(self.root_path, self.report_save_path)+'/'+self.send_visual_name+'_'+self.date+'.html'
        # print("report copy succeeded")
        # # df.to_excel(self.new_report, index=False)

    def to_string(self):
        return f'''message: {self.message}
                subject: {self.subject}
                header_from: {self.header_from}
                sender_email: {self.sender_email}
                header_to: {self.header_to}
                recipient_show: {self.recipient_show}
                cc_show: {self.cc_show}
                user: {self.user}
                to_addrs: {self.to_addrs}
                temp_save_path: {self.email_company}
                read_file: {self.read_file}
                read_file_format: {self.read_file_format}
                end_dt: {self.end_dt}
                send_file: {self.send_file}
                send_file_format: {self.send_file_format}
                '''


    def send_email(self):
        print(self.to_string())
        self. _get_attachment_path()
        
        # return 
        # Create the MIMEText object
        # email = MIMEText(self.message, 'plain', _charset="utf-8")
        msg = MIMEMultipart()
        self.subject = self.subject + f'-{self.end_dt}'
        msg["Subject"] = Header(self.subject, "utf-8")

        if self.email_company == 'qq':
            # Format the 'From' header
            if all(ord(c) < 128 for c in self.header_from):
                from_header = f"{self.header_from} <{self.sender_email}>"
        else:
            nickname_encoded = base64.b64encode(self.header_from.encode('utf-8')).decode('ascii')
            from_header = f"=?utf-8?B?{nickname_encoded}?= <{self.sender_email}>"

        msg["From"] = formataddr((Header(self.header_from).encode(), self.user))
        # msg["From"] = from_header
        
        if self.email_company == 'gmail':
            msg["From"] = self.sender_email

        msg["To"] = Header(self.header_to, "utf-8")
        msg["Cc"] = Header(self.cc_show, "utf-8")
        all_recipients = self.to_addrs.split(',') + self.cc_show.split(',')

        msg.attach(MIMEText(self.message, 'plain', _charset="utf-8"))

        for attachment in self.attachments:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(attachment, "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
            msg.attach(part)


        # # attach report file
        # part = MIMEBase('application', "octet-stream")
        # part.set_payload(open(self.new_report, "rb").read())
        # encoders.encode_base64(part)
        # part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(self.new_report))
        # msg.attach(part)

        # # attach visual file
        # part = MIMEBase('application', "octet-stream")
        # part.set_payload(open(self.new_visual, "rb").read())
        # encoders.encode_base64(part)
        # part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(self.new_visual))
        # msg.attach(part)
        # # print(msg.as_string())

        # Use SMTP_SSL for secure email sending
        with SMTP_SSL(host="smtp.exmail.qq.com", port=465) as smtp:
            smtp.login(user=self.user, password=self.password)
            smtp.sendmail(from_addr=self.user, to_addrs=all_recipients, msg=msg.as_string())
            print('Email sent successfully')
