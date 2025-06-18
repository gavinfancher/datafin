import smtplib
import io
from typing import Union, List, Optional
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
import pandas as pd

class GmailClient:
    """
    """
    
    def __init__(
            self,
            sender_email: str,
            app_password: str
    ):
        """
        """
        self.sender_email = sender_email
        self.app_password = app_password
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        
        self._test_connection()
    
    def _test_connection(self) -> bool:
        
        """
        docs
        """
        
        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender_email, self.app_password)
            server.quit()
            return True
        except Exception as e:
            raise Exception(f"Failed to connect to Gmail SMTP: {str(e)}")
    
    def send_email(
            self, 
            to: Union[str, List[str]], 
            subject: str, 
            text: str, 
            png_dict: Optional[dict[str, bytes]] = None,
            df: Optional[pd.DataFrame] = None,
            csv_filename: Optional[str] = None
    ) -> None:
        
        """
        """
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(to) if isinstance(to, list) else to
            msg['Subject'] = subject
            
            msg.attach(MIMEText(text, 'plain'))
            
            if df is not None:
                self._attach_dataframe_as_csv(msg, df, csv_filename)
            
            if png_dict is not None:
                self._attach_png_dict(msg, png_dict)
            
            recipients = []
            if isinstance(to, list):
                recipients.extend(to)
            else:
                recipients.append(to)
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender_email, self.app_password)
            
            text_msg = msg.as_string()
            server.sendmail(self.sender_email, recipients, text_msg)
            server.quit()
            
        except Exception as e:
            raise Exception(f"Failed to send email: {str(e)}")

    
    def _attach_dataframe_as_csv(
            self,
            msg: MIMEMultipart,
            df: pd.DataFrame,
            filename: Optional[str] = None
    ) -> None:
        
        """
        """
        
        if filename is None:
            filename = "data.csv"
        
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(csv_data.encode())
        encoders.encode_base64(attachment)
        
        attachment.add_header(
            'Content-Disposition',
            f'attachment; filename= {filename}'
        )
        
        msg.attach(attachment)
    
    def _attach_png_dict(
            self,
            msg: MIMEMultipart,
            png_dict: dict[str, bytes]
    ) -> None:
        """
        """
        for filename, png_bytes in png_dict.items():
            # Ensure filename has .png extension
            if not filename.endswith('.png'):
                filename += '.png'
                
            image_attachment = MIMEImage(png_bytes, _subtype='png')
            image_attachment.add_header(
                'Content-Disposition', 
                f'attachment; filename="{filename}"'
            )
            msg.attach(image_attachment)
    
    def _attach_single_png(
            self,
            msg: MIMEMultipart,
            png_bytes: bytes,
            filename: Optional[str] = None
    ) -> None:
        """
        """
        if filename is None:
            filename = "image.png"
        
        if not filename.endswith('.png'):
            filename += '.png'
        
        img_attachment = MIMEImage(png_bytes, _subtype='png')
        img_attachment.add_header(
            'Content-Disposition',
            f'attachment; filename="{filename}"'
        )
        
        msg.attach(img_attachment)