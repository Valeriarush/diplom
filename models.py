from sqlalchemy import Boolean, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from database import Base
from datetime import datetime

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True, nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    phone = Column(String, nullable=False)
    reschedules_this_month = Column(Integer, default=0)
    cancels_this_month = Column(Integer, default=0)
    last_action_month = Column(Integer)
    bookings = relationship("Booking", back_populates="user", cascade="all, delete-orphan")
    feedbacks = relationship("Feedback", back_populates="user")

class Service(Base):
    __tablename__ = "services"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    price = Column(String, nullable=False)
    description = Column(String)

class Schedule(Base):
    __tablename__ = "schedules"
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False, unique=True, index=True)
    bookings = relationship("Booking", back_populates="schedule", cascade="all, delete-orphan")

class Booking(Base):
    __tablename__ = "bookings"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False) 
    date = Column(DateTime, nullable=False)
    user = relationship("User", back_populates="bookings")
    service_id = Column(Integer, ForeignKey("services.id"), nullable=False)
    confirmed = Column(Boolean, default=True)
    schedule_id = Column(Integer, ForeignKey("schedules.id"), nullable=False)  # Обязательная связь
    schedule = relationship("Schedule", back_populates="bookings") 
    service = relationship("Service")
    reminder_24h_sent = Column(Boolean, default=False)
    reminder_3h_sent = Column(Boolean, default=False)

class Feedback(Base):
    __tablename__ = "feedbacks"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    text = Column(String(500), nullable=False)
    rating = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    user = relationship("User", back_populates="feedbacks")