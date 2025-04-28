from typing import Optional, Dict, Any
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session


class RDSClient:
    def __init__(
        self,
        db_host: Optional[str] = None,
        db_port: Optional[str] = None,
        db_name: Optional[str] = None,
        db_user: Optional[str] = None,
        db_password: Optional[str] = None,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        use_ssl: bool = True
    ) -> None:
        """
        Initialize RDS client with optional credentials.
        If no credentials are provided, it will use environment variables.
        
        Args:
            db_host: RDS host endpoint
            db_port: RDS port (default: 5432 for PostgreSQL)
            db_name: Database name
            db_user: Database username
            db_password: Database password
            region_name: AWS region name
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            use_ssl: Whether to use SSL for the connection
        """
        self.db_host = db_host or os.getenv('RDS_HOST')
        self.db_port = db_port or os.getenv('RDS_PORT', '5432')
        self.db_name = db_name or os.getenv('RDS_DB_NAME')
        self.db_user = db_user or os.getenv('RDS_USERNAME')
        self.db_password = db_password or os.getenv('RDS_PASSWORD')
        self.region_name = region_name or os.getenv('AWS_REGION')
        self.aws_access_key_id = aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if not all([self.db_host, self.db_port, self.db_name, self.db_user, self.db_password]):
            raise ValueError("Missing required database connection parameters")
            
        self.engine = self._create_engine(use_ssl)
        self.Session = sessionmaker(bind=self.engine)
        
    def _create_engine(self, use_ssl: bool) -> Engine:
        """
        Create SQLAlchemy engine with the provided credentials
        
        Args:
            use_ssl: Whether to use SSL for the connection
            
        Returns:
            SQLAlchemy engine
        """
        ssl_mode = "require" if use_ssl else "disable"
        
        connection_string = (
            f"postgresql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
            f"?sslmode={ssl_mode}"
        )
        
        return create_engine(connection_string)
        
    def get_session(self) -> Session:
        """
        Get a new database session
        
        Returns:
            SQLAlchemy session
        """
        return self.Session()
        
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> list:
        """
        Execute a SQL query and return the results
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            List of results
        """
        with self.get_session() as session:
            result = session.execute(text(query), params or {})
            return [dict(row) for row in result]
            
    def execute_many(self, query: str, params_list: list) -> None:
        """
        Execute a SQL query multiple times with different parameters
        
        Args:
            query: SQL query string
            params_list: List of parameter dictionaries
        """
        with self.get_session() as session:
            for params in params_list:
                session.execute(text(query), params)
            session.commit()
            
    def close(self) -> None:
        """
        Close the database connection
        """
        self.engine.dispose()
