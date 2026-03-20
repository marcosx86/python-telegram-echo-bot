import os
import logging
from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, ForeignKey, create_engine, func
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# Configure logger for this module
logger = logging.getLogger(__name__)

# Global variables to be initialized via setup_database
engine = None
SessionLocal = None
Base = declarative_base()

def setup_database(database_url):
    """
    Initialize the global SQLAlchemy engine and SessionLocal with a specific URL.
    
    Args:
        database_url (str): The connection string for the database.
        
    Returns:
        tuple: (engine, SessionLocal)
    """
    global engine, SessionLocal
    
    # SQLAlchemy 1.4+ removed support for 'postgres://' schema, preferring 'postgresql://'
    if database_url and database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, SessionLocal

class User(Base):
    """
    Represents a Telegram user in the database.
    
    Attributes:
        id (int): Primary key.
        telegram_id (int): Unique Telegram User ID.
        first_name (str): User's first name.
        last_name (str): User's last name.
        username (str): User's Telegram username.
        files (relationship): List of files owned by the user.
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True, nullable=False)
    first_name = Column(String)
    last_name = Column(String)
    username = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    
    files = relationship("File", back_populates="owner")

    def __repr__(self):
        return f"<User(telegram_id={self.telegram_id}, username='{self.username}')>"

class File(Base):
    """
    Represents a media file (photo/video) received by the bot.
    
    Attributes:
        id (int): Primary key.
        file_id (str): Telegram's file_id.
        file_unique_id (str): Telegram's unique file identifier.
        file_type (str): Type of media ('photo', 'video').
        sha256 (str): SHA256 hash of the file content.
        local_path (str): Local path where the file is stored.
        timestamp (datetime): When the file was received.
        user_id (int): Foreign key to the owner.
    """
    __tablename__ = "files"

    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(String, nullable=False)
    file_unique_id = Column(String, unique=True, nullable=False)
    file_type = Column(String)  # 'photo', 'video', etc.
    sha256 = Column(String, index=True)
    local_path = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, server_default=func.now())
    
    user_id = Column(Integer, ForeignKey("users.id"))
    owner = relationship("User", back_populates="files")

    def __repr__(self):
        return f"<File(file_type='{self.file_type}', sha256='{self.sha256}', timestamp='{self.timestamp}')>"

# Create tables
def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()

class DatabaseManager:
    """
    High-level manager for database operations.
    Handles user registration and file metadata storage.
    """
    def __init__(self):
        """Initializes the manager. Tables should be created via migrations."""
        pass

    def get_session(self):
        """
        Creates and returns a new database session.
        
        Raises:
            Exception: If setup_database has not been called.
        """
        if SessionLocal is None:
            raise Exception("Database not initialized. Please call setup_database(url) first.")
        return SessionLocal()

    def register_user(self, telegram_user):
        """
        Register a new user or update an existing one based on Telegram user data.
        Handles race conditions by catching IntegrityError.
        
        Args:
            telegram_user (telebot.types.User): The user object from telebot.
            
        Returns:
            User: The saved or updated User model instance.
        """
        session = self.get_session()
        try:
            # First attempt: Check by Telegram ID
            logger.debug(f"Registering user: {telegram_user}")
            user = session.query(User).filter(User.telegram_id == telegram_user.id).first()
            
            # If not found by ID, check by username (per user request)
            if not user and telegram_user.username:
                logger.debug(f"User not found by ID, checking by username: {telegram_user.username}")
                user = session.query(User).filter(User.username == telegram_user.username).first()
                if user:
                    logger.debug(f"User found by username: {user}")
                    # If found by username, update the telegram_id (if it changed or was unknown)
                    user.telegram_id = telegram_user.id

            if not user:
                logger.debug(f"Creating new user: {telegram_user}")
                # Create new user
                user = User(
                    telegram_id=telegram_user.id,
                    first_name=telegram_user.first_name,
                    last_name=telegram_user.last_name,
                    username=telegram_user.username
                )
                logger.info(f"User registered: {user}")
                session.add(user)
            else:
                logger.debug(f"Updating existing user: {user}")
                # Update existing user
                user.first_name = telegram_user.first_name
                user.last_name = telegram_user.last_name
                user.username = telegram_user.username
                logger.info(f"User updated: {user}")
            
            try:
                session.commit()
            except Exception:
                # If commit fails (e.g., due to race condition), rollback and try fetching one last time
                session.rollback()
                user = session.query(User).filter(User.telegram_id == telegram_user.id).first()
                if user:
                    user.first_name = telegram_user.first_name
                    user.last_name = telegram_user.last_name
                    user.username = telegram_user.username
                    session.commit()
                else:
                    raise # Rethrow if it's a different kind of error
            
            session.refresh(user)
            return user
        finally:
            session.close()

    def get_user_by_username(self, username):
        """
        Fetch a user from the database by their username.
        
        Args:
            username (str): The Telegram username.
            
        Returns:
            User: The User instance if found, otherwise None.
        """
        session = self.get_session()
        try:
            return session.query(User).filter(User.username == username).first()
        finally:
            session.close()

    def get_file_by_hash(self, user_id, sha256):
        """
        Check if a file with the given SHA256 already exists for a specific user.
        
        Args:
            user_id (int): The internal database ID of the user.
            sha256 (str): The SHA256 hash of the file.
            
        Returns:
            File: The File instance if found, otherwise None.
        """
        session = self.get_session()
        try:
            logger.debug(f"Checking for file with SHA256: {sha256} for user: {user_id}")
            return session.query(File).filter(
                File.user_id == user_id,
                File.sha256 == sha256
            ).first()
        finally:
            session.close()

    def save_file_metadata(self, telegram_user_id, file_id, file_unique_id, file_type, sha256=None, local_path=None):
        """
        Save file metadata associated with a specific Telegram user.
        
        Args:
            telegram_user_id (int): The Telegram ID of the owner.
            file_id (str): Telegram's file_id.
            file_unique_id (str): Telegram's unique file identifier.
            file_type (str): Media type.
            sha256 (str, optional): SHA256 checksum.
            local_path (str, optional): Local file path.
            
        Returns:
            File: The saved File model instance or None if user not found.
        """
        session = self.get_session()
        try:
            logger.debug(f"Saving file metadata for user: {telegram_user_id}")
            user = session.query(User).filter(User.telegram_id == telegram_user_id).first()
            if not user:
                # Should not happen if registered on message, but safety first
                logger.error(f"User not found: {telegram_user_id}")
                return None
            
            new_file = File(
                file_id=file_id,
                file_unique_id=file_unique_id,
                file_type=file_type,
                sha256=sha256,
                local_path=local_path,
                user_id=user.id
            )
            logger.debug(f"New file: {new_file}")
            session.add(new_file)
            session.commit()
            session.refresh(new_file)
            return new_file
        finally:
            session.close()
