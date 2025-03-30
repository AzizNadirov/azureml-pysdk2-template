import os
from pathlib import Path
import requests
from typing import List, Optional, Union

from azure.ai.ml import MLClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential

from email.message import EmailMessage
import smtplib

from dotenv import dotenv_values
from loguru import logger


def get_ml_client(from_config: Path = 'config.json', 
                  from_dotenv: Path = '.env') -> MLClient:
    """ 
    Returns ml_client object from the provided creds.  
        Params:
            - from_config: Path | str - config file path of the workspace, can be downloaded from aml site. README_TEMPALTE.MD
            - from_dotenv: Path | str - .env file path with `SUBSCRIPTION_ID`, `RESOURCE_GROUP`, `WORKSPACE_NAME` variables.
        Returns:
            MLClient - client object for the aml workspace
        Raises:
            ValueError - if any issue with passed args
    """
    from_config = Path(from_config).resolve()
    from_dotenv = Path(from_dotenv).resolve()
    logger.debug(f"config file '{from_config.as_posix()}' exists: {from_config.exists()}")
    logger.debug(f".env file '{from_dotenv.as_posix()}' exists: {from_dotenv.exists()}")
    try:
        credential = DefaultAzureCredential()
    except Exception as e:
        credential = InteractiveBrowserCredential()
    # try config:
    if from_config.exists():
        return MLClient.from_config(credential=credential, path=from_config.as_posix())
    # .env
    elif from_dotenv.exists():
        logger.debug(f"Using .env file. File '{from_dotenv.as_posix()}'.")
        de_values = dotenv_values(from_dotenv)
        try:
            return MLClient(
                    credential=credential,
                    subscription_id=de_values["SUBSCRIPTION_ID"],
                    resource_group_name=de_values["RESOURCE_GROUP"],
                    workspace_name=de_values["WORKSPACE_NAME"])
        except KeyError:
            raise ValueError("Your .env file have to contain 'SUBSCRIPTION_ID', 'RESOURCE_GROUP', 'WORKSPACE_NAME' variables!")
    else:
        raise ValueError(f"You have to provide existing one of 'from_config' or 'from_dotenv' methods.")
        


def get_secret(secret_name: str, 
               keyvault_name="ds-ml", 
               credential=None) -> str:
    """ get secret from keyvault """
    if not credential:
        credential = DefaultAzureCredential()
    logger.info(f"Getting secret: {secret_name}; keyvault_name: {keyvault_name}")
    vault_url = f"https://{keyvault_name}.vault.azure.net/"
    secret_client = SecretClient(vault_url=vault_url, credential=credential)
    secret = secret_client.get_secret(secret_name)
    return secret.value


def send_mail_absolute(subject: str, 
                       body: str, 
                       to: list[str], 
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
