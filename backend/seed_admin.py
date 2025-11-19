"""
Seed admin user if not exists
"""
from sqlalchemy.orm import Session
from database import SessionLocal
from models.users import User
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def seed_admin():
    db: Session = SessionLocal()
    admin = db.query(User).filter(User.username == "admin").first()
    if not admin:
        admin = User(
            username="admin",
            role="Admin",
            password_hash=pwd_context.hash("admin@123"),
            email="admin@example.com",
            display_name="Administrator"
        )
        db.add(admin)
        db.commit()
        print("Admin user created with password: admin@123")
    else:
        print("Admin user already exists.")
    db.close()

if __name__ == "__main__":
    seed_admin()
