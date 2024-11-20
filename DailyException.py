
from calendar import day_name
import pyodbc
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.mime.text import MIMEText
import os
from datetime import date
import datetime
from oauthlib.uri_validate import query
from pandas import read_csv, merge
from datetime import date, timedelta


server        = '172.25.6.249'
database      = 'MIS'
username      = 'ewadm'
password      = 'EW@dmin#2022'
driver        = '{ODBC Driver 17 for SQL Server}'
smtp_server   = 'titan-co-in.mail.protection.outlook.com'
smtp_port     = 25
from_email    = 'noreply-timesheet@titan.co.in'
to_email = 'sivaramakrishnan@titan.co.in'

# server = '172.25.6.249'
# database = 'MIS'
# username = 'ewadm'                                             #comment************************************
# password = 'EW@dmin#2022'
# driver = '{ODBC Driver 17 for SQL Server}'
# smtp_server = 'smtp.office365.com'
# smtp_port = 587
# smtp_user = 'naveenn@titan.co.in'
# smtp_password = 'm0tivate@W2rk'
# from_email = smtp_user
# to_email = 'prantapaul@titan.co.in'



today_date = date.today().strftime('%Y/%m/%d')
current_date = date.today().strftime('%d/%m/%Y')



query_for_all = "SELECT activity [Key Focus Area], division [Business Area],project_name [Project], activity_type [Activity], duration [Time Spent], remarks [Remarks], b.Name [Name], [day] [Date] from tbl_tsdata as a inner join tbl_usermapping b on a.username = b.Username where [day]= '%s' " %today_date
query_for_NameAndTime = "select a.* from (SELECT NameMapping.Name, SUM(CAST(TRY_CAST(TimeSheetData.duration AS FLOAT) AS FLOAT)) as Time FROM tbl_tsdata TimeSheetData JOIN tbl_usermapping NameMapping ON TimeSheetData.username = NameMapping.Username where TimeSheetData.day= '%s' group by NameMapping.Name ) a where a.Time < 8" %today_date
query_for_cc = "select a.EmailID from (SELECT NameMapping.Name,NameMapping.EmailID, SUM(CAST(TRY_CAST(TimeSheetData.duration AS FLOAT) AS FLOAT)) as Time FROM tbl_tsdata TimeSheetData JOIN tbl_usermapping NameMapping ON TimeSheetData.username = NameMapping.Username where TimeSheetData.day= '%s' group by NameMapping.Name,NameMapping.EmailID ) a where a.Time < 8" %today_date


if (date.today().weekday() ) < 5:
    query_for_zeroTimePeople = "select Name, 0 AS Time from tbl_usermapping where Name not in (SELECT NameMapping.Name FROM tbl_tsdata TimeSheetData JOIN tbl_usermapping NameMapping ON TimeSheetData.username = NameMapping.Username where TimeSheetData.day= '%s' group by NameMapping.Name) " %today_date
    query_for_CC_zeroTimePeople = "select tbl_usermapping.EmailID from tbl_usermapping where Name not in (SELECT NameMapping.Name FROM tbl_tsdata TimeSheetData JOIN tbl_usermapping NameMapping ON TimeSheetData.username = NameMapping.Username where TimeSheetData.day= '%s' group by NameMapping.Name )" %today_date
else:
    query_for_zeroTimePeople = ""
    query_for_CC_zeroTimePeople = ""


def fetch_data():
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(conn_str)
    data_for_all = pd.read_sql(query_for_all, conn)
    data_for_NameAndTime = pd.read_sql(query_for_NameAndTime, conn)
    conn.close()
    return data_for_all, data_for_NameAndTime


def fetch_data_CC(name_of_Query):
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(name_of_Query, conn)
    conn.close()
    return df


def save_to_csv(df, name_of_csv):
    csv_file = name_of_csv + '.csv'
    df.to_csv(csv_file, index=False)
    return csv_file


def send_email_with_attachment(subject, body, attachment_path, unique_cc_str):
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg['Cc'] = unique_cc_str

    msg.attach(MIMEText(body, 'html'))

    part = MIMEBase('application', 'octet-stream')
    with open(attachment_path, 'rb') as attachment:
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment_path)}')
        msg.attach(part)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        #server.login(smtp_user, smtp_password)
        server.send_message(msg)


def main():
    data_for_all, data_for_NameAndTime = fetch_data()
    data_for_cc = fetch_data_CC(query_for_cc)

    csv_file_for_all = save_to_csv(data_for_all, "Daily_Exception_Detail")
    csv_file_for_NameAndTime = save_to_csv(data_for_NameAndTime, "NameAndTime")
    csv_file_for_cc = save_to_csv(data_for_cc, "CC_Detail")

    csv_content1 = read_csv(csv_file_for_NameAndTime)
    html_table1 = csv_content1.to_html(index=False)

    cc = pd.read_csv(csv_file_for_cc)
    cc_list = list(cc["EmailID"])

    cc_str= " "

    for i in range(len(cc_list)):
        if i == len(cc_list) - 1:
            cc_str = cc_str + cc_list[i]
        else:
            cc_str = cc_str + cc_list[i] + ","

    if query_for_zeroTimePeople != "":
        data_for_zeroTimePeople = fetch_data_CC(query_for_zeroTimePeople)
        data_for_cc_zeroTimePeople = fetch_data_CC(query_for_CC_zeroTimePeople)

        csv_file_for_ccZeroHour = save_to_csv(data_for_cc_zeroTimePeople, "CC_ZeroHour")
        csv_file_for_zeroTimePeople = save_to_csv(data_for_zeroTimePeople, "ZeroTimePeople")

        csv_content2 = read_csv(csv_file_for_zeroTimePeople)
        html_table2 = csv_content2.to_html(index=False)

        cc_zeroHour = pd.read_csv(csv_file_for_ccZeroHour)
        cc_zeroHour_list = list(cc_zeroHour["EmailID"])

        if len(cc_zeroHour_list) > 0:
            if len(cc_str) > 0:
                cc_str = cc_str + ","

        for i in range(len(cc_zeroHour_list)):
            if i == len(cc_zeroHour_list) - 1:
                cc_str = cc_str + cc_zeroHour_list[i]
            else:
                cc_str = cc_str + cc_zeroHour_list[i] + ","

    if (len(cc_str) == 0):
        cc_str = 'naveenn@titan.co.in'
    else:
        cc_str = cc_str + "," + 'naveenn@titan.co.in'

    # The below part is to remove mail id duplicates(Naveen sir mail id was coming multiple time some cases)

    email_list = [email.strip() for email in cc_str.split(',')]
    unique_emails = set(email_list)
    unique_cc_str = ','.join(unique_emails)  # this should be passed to send_email function

    subject = 'Daily Timesheet Exception Report'

    body = f"""\n 
            <html>
                <head>
                    <style>
                        table {{ border-collapse: collapse; width: 40%; }}
                        th, td {{ border: 1px solid black; padding: 8px; text-align: left; }} 
                        th {{ background-color: #E8E8E8; }}    
                    </style>
                </head>
                 <body>
                   {f"<h4>Below is the timesheet exception report for {current_date} </h4> </br> {html_table1}" if (len(data_for_NameAndTime) != 0) else "" }                   

                    </br>

                    {f"No exception found today!" if len(data_for_zeroTimePeople) == 0 and len(data_for_NameAndTime) == 0 else ""}

                    {f"<h4>People who didn't make any Time-sheet entry: </h4><b>{html_table2}</b></br>" if (len(data_for_zeroTimePeople) != 0) else ""}

                    Thank you. Have a good day!
                </h4>

                <h4>This is a system generated email. Please do not reply to this message.</h4>
            </body>
            </html>

        """

    try:
        send_email_with_attachment(subject, body, csv_file_for_all, unique_cc_str)
        #print(body)
        #print(unique_cc_str)
        print('Email sent successfully.')

    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()







