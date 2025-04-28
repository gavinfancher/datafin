import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, List

class RDSClient:
    def __init__(
        self,
        host_connection_name: str,
        user: str,
        password: str,
        database: Optional[str] = None,
        port: int = 3306
    ) -> None:
        
        """
        docs
        """

        self.host = host_connection_name
        self.user = user
        self.password = password
        self.database = database
        self.port = port

        self.engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database if self.database else ''}"
        )
        
        if self.database:
            self.use_database(self.database)


    #######################################################
    #######################################################
        

    def list_databases(self) -> List[str]:
        """
        List all available databases
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("SHOW DATABASES"))
            return [row[0] for row in result]
    

    #######################################################


    def list_tables(self) -> List[str]:
        """
        docs
        """
        if not self.database:
            raise ValueError("No database selected. Use use_database() first.")
            
        with self.engine.connect() as conn:
            result = conn.execute(text("SHOW TABLES"))
            return [row[0] for row in result]
    

    #######################################################


    def use_database(self, database: str) -> None:
        
        """
        docs
        """
        
        self.database = database
        self.engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
    

    #######################################################


    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """
        Drop a table from the current database.
        
        Args:
            table_name: Name of the table to drop
            if_exists: If True, adds IF EXISTS clause to prevent errors if table doesn't exist
        """
        if not self.database:
            raise ValueError("No database selected. Use use_database() first.")
            
        if_exists_clause = "IF EXISTS" if if_exists else ""
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE {if_exists_clause} {table_name}"))
            conn.commit()


    #######################################################
     
       
    def query(self, query: str) -> pd.DataFrame:
        """
        docs
        """
        if not self.database:
            raise ValueError("No database selected. Use use_database() first.")
            
        return pd.read_sql(query, self.engine)


    #######################################################


    def close(self) -> None:
        """
        docs
        """
        if self.engine:
            self.engine.dispose()