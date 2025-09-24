"""
Email service for sending invitations and notifications.
"""

import logging
from typing import Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os

logger = logging.getLogger(__name__)

class EmailService:
    def __init__(self):
        self.smtp_host = os.getenv("SMTP_HOST", "")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USER", "")
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.from_email = os.getenv("FROM_EMAIL", "noreply@winkai.in")
        
        # Check if email is configured
        self.is_configured = bool(self.smtp_host and self.smtp_user and self.smtp_password)
        
        if not self.is_configured:
            logger.warning("Email service not configured. Set SMTP_HOST, SMTP_USER, and SMTP_PASSWORD environment variables.")
    
    def send_email(self, to_email: str, subject: str, html_body: str, text_body: Optional[str] = None) -> bool:
        """Send an email."""
        if not self.is_configured:
            logger.warning(f"Email service not configured, skipping email to {to_email}")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = to_email
            
            # Add text part
            if text_body:
                part1 = MIMEText(text_body, 'plain')
                msg.attach(part1)
            
            # Add HTML part
            part2 = MIMEText(html_body, 'html')
            msg.attach(part2)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email to {to_email}: {e}")
            return False

# Global email service instance
email_service = EmailService()

def send_invite_email(to_email: str, invite_token: str, store_name: str, invited_by: str) -> bool:
    """Send invitation email to new user."""
    subject = f"You're invited to join {store_name} on Wink AI"
    
    # Create invite URL (you'll need to adjust this based on your frontend URL)
    invite_url = f"https://winkai.in/accept-invite?token={invite_token}"
    
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px;">
          <h2 style="color: #333;">Welcome to Wink AI!</h2>
          <p>Hi there!</p>
          <p><strong>{invited_by}</strong> has invited you to join <strong>{store_name}</strong> on Wink AI.</p>
          <p>Wink AI is a powerful retail analytics platform that helps businesses understand customer behavior through AI-powered video analysis.</p>
          
          <div style="text-align: center; margin: 30px 0;">
            <a href="{invite_url}" 
               style="background-color: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">
              Accept Invitation
            </a>
          </div>
          
          <p>If the button doesn't work, you can copy and paste this link into your browser:</p>
          <p style="word-break: break-all; background-color: #f1f3f4; padding: 10px; border-radius: 4px;">
            {invite_url}
          </p>
          
          <p style="margin-top: 30px; font-size: 14px; color: #666;">
            This invitation will expire in 7 days. If you don't want to join this store, you can safely ignore this email.
          </p>
          
          <hr style="border: none; border-top: 1px solid #ddd; margin: 30px 0;">
          <p style="font-size: 12px; color: #999; text-align: center;">
            This email was sent by Wink AI. If you have questions, please contact support.
          </p>
        </div>
      </body>
    </html>
    """
    
    text_body = f"""
    Welcome to Wink AI!
    
    {invited_by} has invited you to join {store_name} on Wink AI.
    
    To accept this invitation, visit: {invite_url}
    
    This invitation will expire in 7 days.
    
    If you don't want to join this store, you can safely ignore this email.
    """
    
    return email_service.send_email(to_email, subject, html_body, text_body)

def send_password_reset_email(to_email: str, reset_token: str) -> bool:
    """Send password reset email."""
    subject = "Reset your Wink AI password"
    
    reset_url = f"https://winkai.in/reset-password?token={reset_token}"
    
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px;">
          <h2 style="color: #333;">Reset Your Password</h2>
          <p>Hi!</p>
          <p>You requested to reset your password for your Wink AI account.</p>
          
          <div style="text-align: center; margin: 30px 0;">
            <a href="{reset_url}" 
               style="background-color: #dc3545; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">
              Reset Password
            </a>
          </div>
          
          <p>If the button doesn't work, you can copy and paste this link into your browser:</p>
          <p style="word-break: break-all; background-color: #f1f3f4; padding: 10px; border-radius: 4px;">
            {reset_url}
          </p>
          
          <p style="margin-top: 30px; font-size: 14px; color: #666;">
            This link will expire in 1 hour. If you didn't request this password reset, you can safely ignore this email.
          </p>
          
          <hr style="border: none; border-top: 1px solid #ddd; margin: 30px 0;">
          <p style="font-size: 12px; color: #999; text-align: center;">
            This email was sent by Wink AI. If you have questions, please contact support.
          </p>
        </div>
      </body>
    </html>
    """
    
    text_body = f"""
    Reset Your Password
    
    You requested to reset your password for your Wink AI account.
    
    To reset your password, visit: {reset_url}
    
    This link will expire in 1 hour. If you didn't request this password reset, you can safely ignore this email.
    """
    
    return email_service.send_email(to_email, subject, html_body, text_body)