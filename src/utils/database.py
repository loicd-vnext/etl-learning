"""
Database utility for connection management
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Optional
from dotenv import load_dotenv

from src.utils.logger import logger

# Load environment variables
load_dotenv()


class DatabaseManager:
    """Manage database connections and operations"""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize database manager
        
        Args:
            connection_string: Database connection string. If None, will use environment variables
        """
        if connection_string:
            self.connection_string = connection_string
        else:
            self.connection_string = self._get_connection_string()
        
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[sessionmaker] = None
    
    def _get_connection_string(self) -> str:
        """
        Get connection string from environment variables
        
        Supports both PostgreSQL and SQLite
        """
        db_type = os.getenv("DB_TYPE", "postgresql").lower()
        
        if db_type == "postgresql":
            host = os.getenv("DB_HOST", "localhost")
            # Use 127.0.0.1 instead of localhost to avoid IPv6 issues
            if host == "localhost":
                host = "127.0.0.1"
            port = os.getenv("DB_PORT", "5432")
            database = os.getenv("DB_NAME", "etl_demo")
            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASSWORD", "")
            
            if not password:
                logger.warning("DB_PASSWORD not set. Using empty password (not recommended for production)")
            
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        else:  # SQLite (fallback option)
            db_path = os.getenv("DB_PATH", "data/etl_demo.db")
            # Ensure directory exists
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            return f"sqlite:///{db_path}"
    
    def connect(self) -> Engine:
        """Create and return database engine"""
        if self.engine is None:
            try:
                self.engine = create_engine(
                    self.connection_string,
                    echo=False,  # Set to True for SQL query logging
                    pool_pre_ping=True  # Verify connections before using
                )
                logger.info(f"Database connection established: {self._mask_connection_string()}")
            except Exception as e:
                logger.error(f"Failed to connect to database: {e}")
                raise
        
        return self.engine
    
    def _mask_connection_string(self) -> str:
        """Mask password in connection string for logging"""
        if "postgresql://" in self.connection_string:
            parts = self.connection_string.split("@")
            if len(parts) == 2:
                return f"postgresql://***@{parts[1]}"
        return "sqlite:///***"
    
    def get_session(self) -> Session:
        """Get database session"""
        if self.SessionLocal is None:
            engine = self.connect()
            self.SessionLocal = sessionmaker(bind=engine)
        
        return self.SessionLocal()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            engine = self.connect()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


# Global database manager instance
db_manager = DatabaseManager()

