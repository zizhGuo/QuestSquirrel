import os
import shutil
import platform

SYSTEM_TYPE = platform.system()

import pandas as pd
CUR_PATH = os.path.dirname(os.path.abspath(__file__))
print(os.path.abspath(__file__))
print(os.path.dirname(os.path.abspath(__file__)))

from smtplib import SMTP_SSL
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.header import Header
from email.utils import formataddr
from email import encoders
from smtplib import SMTP_SSL
import base64

# python type placeholder
from typing import List

from .validator import Validator


class EmailScheduler:
    def __init__(self, config, root_path, module) -> None:
        self.emails = []
        for sub_email, _sub_config in config[module.email_module].items():
            _ = Email(config, root_path, module, sub_email)
            self.emails.append(_)

    def send_email(self):
        for i in range(len(self.emails)):
            print('run send email')
            try:
                print(f'sending email: {i}')
                self.emails[i].send_email()
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
            except Exception as e:
                print(f'send email failed. {i}')
                print(e)


class EmailConfigValidator(Validator):
    def __init__(self, name):
        super(EmailConfigValidator, self).__init__(name)
    def validate(self, value):
        # Example validation: only integers are allowed
        if not isinstance(value, int):
            raise ValueError("Only integers are allowed")

class AttachmentListValidator(Validator):
    def __init__(self, name):
        super(AttachmentListValidator, self).__init__(name)

    def validate(self, value):
        """
            验证文件是否存在
        """
        if os.path.exists(value):
            pass
        else:
            # print("File does not exist.")
            raise ValueError("File does not exist.")

class AttachmentList(list):
    def __init__(self, name, *args):
        super().__init__(*args)
        # 仅复用validator
        self.validator = AttachmentListValidator(name)

    def append(self, value):
        self.validator.validate(value)
        super().append(value)

    def extend(self, iterable):
        for item in iterable:
            self.validator.validate(item)
        super().extend(iterable)

    def insert(self, index, value):
        self.validator.validate(value)
        super().insert(index, value)

    def __setitem__(self, index, value):
        if isinstance(index, slice):
            # If setting a slice, validate each item in the value iterable
            for item in value:
                self.validator.validate(item)
        else:
            # Single item assignment
            self.validator.validate(value)
        super().__setitem__(index, value)



"""
两个可能的exception:
1. format error, 使用validator个性化定制， 比如list元素数量，扩展名称等
2. 文件不存在

第一种exception在validator里处理：
1. 邮件相关信息错误，中止程序，发送邮件通知本人，邮件内容包含错误信息
2. 文件格式错误，继续处理，仅添加正确的文件到附件列表

对于实际初始化附件列表时，如果文件不存在，直接跳过，不添加到附件列表
处理：
1. 文件不存在，发送邮件通知本人，邮件内容包含错误信息


TODO
1. 需要发送邮件通知本人的切面装饰器
2. 需要logging记录当前模块上下文信息，用于添加日志到邮件内容中

"""

class Email:
    # def __init__(self, message, subject, header_from, header_to, sender_email, recipient_show, cc_show, user, password, to_addrs, email_company, attch_root_dir, output_file_name):
    def __init__(self, config: dict, root_path, module, sub_email):
        # self.email_config = config

        # TODO put config[module.email_module][sub_email] a descriptor, validate the format
        try:
            self.module = module
            self.message = config[module.email_module][sub_email]['message']
            self.subject = config[module.email_module][sub_email]['subject']
            self.header_from = config[module.email_module][sub_email]['header_from']
            self.sender_email = config[module.email_module][sub_email]['sender_email']
            self.header_to = config[module.email_module][sub_email]['header_to']
            self.recipient_show = config[module.email_module][sub_email]['recipient_show']
            self.cc_show = config[module.email_module][sub_email]['cc_show']
            self.user = config[module.email_module][sub_email]['user']
            self.password = config[module.email_module][sub_email]['password']
            self.to_addrs = config[module.email_module][sub_email]['to_addrs']
            self.email_company = config[module.email_module][sub_email]['email_company']
        except Exception as e:
            print('init email config failed')
            print(e)
            return
            
        # TODO put report as descriptor, self.report format validate
        self.report = config[module.email_module][sub_email]['attachment_file_read_save']
        self.root_path = root_path
        self.end_dt = config['end_dt']

        self.attachments = AttachmentList('nothing')
        self._init_attachment()
        print(self.attachments)

        return 
        # init report result directory
        self.temp_save_path = config[module.email_module][sub_email]['temp_save_path']
        self.read_file = config[module.email_module][sub_email]['read_file']
        self.read_file_format = config[module.email_module][sub_email]['read_file_format']

        self.root_path = root_path
        self.end_dt = config['end_dt']

        # init email sending report name

        # dt = '20231213'
        self.send_file = config[module.email_module][sub_email]['send_file']
        # self.send_file_name = '{}_{}.xlsx'.format(self.send_file_name, dt)
        self.send_file_format = config[module.email_module][sub_email]['send_file_format']
        # self.send_visual_name = '{}_{}.html'.format(self.send_visual_name, dt)
    
    def _init_attachment(self):
        for temp_path, file in self.report.items():
            for _file in file:
                dir_path = os.path.join(self.root_path, temp_path, self.end_dt)
                file_read = os.path.join(dir_path, _file[0])
                # if file_read not existed, skip, local file applies no valdator
                if isinstance(_file, str):
                    continue
                if _file[1] != 'na':
                    file_write = os.path.join(dir_path, _file[1].split('.')[0]+f'-{self.end_dt}.'+_file[1].split('.')[1])
                    if SYSTEM_TYPE == 'Linux':
                        os.system('cp \'{}\' \'{}\''.format(file_read, file_write))
                    if SYSTEM_TYPE == 'Windows':
                        os.system(f'copy "{file_read}" "{file_write}"') 
                    else:    
                        shutil.copy(file_read, file_write)
                    file_read = file_write
                try:
                    self.attachments.append(file_read)
                except ValueError as e:
                    print(e)
                    print('Attachment append failed: {}'.format(file_read))
                    print('skip this file and continue to next file')
                    # TODO send email to myself


    def send_warning_email(self):
        pass


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
    # TODO
    # add update_message function

    def send_email(self):
        # print(self.to_string())
        # self. _get_attachment_path()
        html = f"""
            <html>
            <head></head>
            <body>
                <p>{self.message}</p>
                <img src="cid:image1">
            </body>
            </html>
        """

        msg = MIMEMultipart()

        for temp_path, file in self.report.items():
            if temp_path == "data/quest_pacific":
                for _file in file:
                    print('_file', _file)
                    if isinstance(_file, str):
                        print('find image file')
                        dir_path = os.path.join(self.root_path, temp_path, 'walkthrough')
                        img_path = os.path.join(dir_path, _file)
                        with open(img_path, 'rb') as f:
                            img = MIMEImage(f.read())
                            img.add_header('Content-ID', '<image1>')
                            msg.attach(img)

        # return 
        # Create the MIMEText object
        # email = MIMEText(self.message, 'plain', _charset="utf-8")
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

        msg.attach(MIMEText(html, 'html'))
        # msg.attach(MIMEText(self.message, 'plain', _charset="utf-8"))

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
