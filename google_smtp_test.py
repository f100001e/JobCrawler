import smtplib

smtp_server = "smtp-relay.gmail.com"
port = 587
sender = "frank@presspassla.com"  # Must match your workspace domain
recipient = "felliott818@gmail.com"

message = f"""\
Subject: Test

This is a test."""

server = smtplib.SMTP(smtp_server, port)
server.starttls()
# Don't call server.login() at all!
server.sendmail(sender, recipient, message)
server.quit()