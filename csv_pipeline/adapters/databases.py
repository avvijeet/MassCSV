from abc import ABC, abstractmethod

from sqlalchemy import create_engine, text

from csv_pipeline.config import Config


class DBAdapter(ABC):
    def __init__(self, config: Config):
        self.config = config
        self.db_type = config.DB_TYPE
        self.db_uri = config.DB_URI
        self.engine = create_engine(self.db_uri)

    @abstractmethod
    def execute_query(self, query, params=None):
        with self.engine.connect() as connection:
            result = connection.execute(text(query), params)
            return result.fetchall()

    @abstractmethod
    def create_tables(self):
        with self.engine.connect() as connection:
            # NOTE: This table can have a data retention policy of year so that,
            # once aggregates are worked upon this data is auto-purged
            # while the aggregates persist in the SalesSummary Table

            # Creating Orders table with indexed columns
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS Orders (
                        OrderID INTEGER PRIMARY KEY,
                        OrderDate DATE,
                        CustomerID TEXT,
                        ProductID TEXT,
                        Quantity INTEGER,
                        UnitPrice REAL,
                        TotalAmount REAL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON Orders (CustomerID);
                    CREATE INDEX IF NOT EXISTS idx_orders_product_id ON Orders (ProductID);
                    CREATE INDEX IF NOT EXISTS idx_orders_order_date ON Orders (OrderDate);
                    CREATE INDEX IF NOT EXISTS idx_orders_customer_product ON Orders (CustomerID, ProductID);
                    """
                )
            )

            # Creating SalesSummary table with indexed columns
            connection.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS SalesSummary (
                        CustomerID TEXT,
                        ProductID TEXT,
                        TotalSales REAL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_sales_summary_customer_id ON SalesSummary (CustomerID);
                    CREATE INDEX IF NOT EXISTS idx_sales_summary_product_id ON SalesSummary (ProductID);
                    CREATE INDEX IF NOT EXISTS idx_sales_summary_total_sales ON SalesSummary (TotalSales);
                    CREATE INDEX IF NOT EXISTS idx_sales_summary_customer_product ON SalesSummary (CustomerID, ProductID);
                    """
                )
            )

    @abstractmethod
    def insert_order(self, order_data):
        with self.engine.connect() as connection:
            connection.execute(
                text(
                    """
                INSERT INTO Orders (OrderID, OrderDate, CustomerID, ProductID, Quantity, UnitPrice, TotalAmount)
                VALUES (:OrderID, :OrderDate, :CustomerID, :ProductID, :Quantity, :UnitPrice, :TotalAmount)
            """
                ),
                order_data,
            )

    @abstractmethod
    def insert_sales_summary(self, summary_data):
        with self.engine.connect() as connection:
            connection.execute(
                text(
                    """
                INSERT INTO SalesSummary (CustomerID, ProductID, TotalSales)
                VALUES (:CustomerID, :ProductID, :TotalSales)
            """
                ),
                summary_data,
            )


class SQLiteAdapter(DBAdapter):
    def execute_query(self, query, params=None):
        return super().execute_query(query=query, params=params)

    def create_tables(self):
        return super().create_tables()

    def insert_order(self, order_data):
        return super().insert_order(order_data=order_data)

    def insert_sales_summary(self, summary_data):
        return super().insert_sales_summary(summary_data=summary_data)


class PostgreSQLAdapter(DBAdapter):
    def execute_query(self, query, params=None):
        return super().execute_query(query=query, params=params)

    def create_tables(self):
        return super().create_tables()

    def insert_order(self, order_data):
        return super().insert_order(order_data=order_data)

    def insert_sales_summary(self, summary_data):
        return super().insert_sales_summary(summary_data=summary_data)
