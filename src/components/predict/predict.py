import os
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from typing import List

from email.message import EmailMessage
import smtplib

def predict(data: pd.DataFrame, model: RandomForestRegressor) -> pd.DataFrame:
    scaler = StandardScaler()
    X = data.drop("Price_euros", axis=1) if "Price_euros" in data.columns.values else data.copy()
    X_scaled = scaler.fit_transform(X)
    preds = model.predict(X_scaled)
    data.loc[:, "predicted_price"] = preds
    return data


def send_mail_absolute(subject: str, 
                       body: str, 
                       to: List[str], 
                       secret_password: str=None,
                       file_path: str=None):
    """ send email with optional attachment 

    Parameters:
        subject: str - email subject
        body: str - email body, text content
        to: list[str] - list of recipients
        file_path: str - path to attachment, file will be attached as attachment
    """
    if not secret_password:
        secret_password = os.environ.get("MAIL_PASSWORD")
        if secret_password is None:
            raise ValueError(f"Failed to extact secret password from environment variable: MAIL_PASSWORD")

    email_address = 'ds.umico.user@gmail.com'
    msg = EmailMessage()
    msg['To'] = to             
    msg['Subject'] = subject
    msg['From'] = email_address
    msg.set_content(body, subtype='plain')

    if file_path:
        with open(file_path, 'rb') as f:
            file = f.read()
        msg.add_attachment(file, maintype='application', subtype='pdf', filename=file_path)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(email_address, secret_password)
        smtp.send_message(msg)