"""
Change admin user password
"""
from sqlalchemy.orm import Session
from database import SessionLocal
from models.users import User
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def change_admin_password(new_password: str):
    db: Session = SessionLocal()
    admin = db.query(User).filter(User.username == "admin").first()
    if admin:
        admin.password_hash = pwd_context.hash(new_password)
        db.commit()
        print(f"Admin password changed to: {new_password}")
    else:
        print("Admin user not found.")
    db.close()

if __name__ == "__main__":
    change_admin_password("admin@123")
