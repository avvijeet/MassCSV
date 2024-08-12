from datetime import datetime
from io import StringIO

import pandas as pd
from adapters import BlobAdapter, DBAdapter


class DataLoader:
    def __init__(self, storage_adapter: BlobAdapter, db_adapter: DBAdapter):
        self.storage_adapter = storage_adapter
        self.db_adapter = db_adapter
        self.error_rows = []

    def log_error(self, row, error_message):
        # Append the error message to the row dictionary
        row["Error"] = error_message
        self.error_rows.append(row)

    def save_error_log(self, error_file_name):
        if self.error_rows:
            # Create a DataFrame from the error rows
            error_df = pd.DataFrame(self.error_rows)
            # Save to a CSV using storage adapter
            csv_data = error_df.to_csv(index=False)
            self.storage_adapter.save_data(error_file_name, csv_data)

    def process_file(self, file_name_with_path: str):
        raw_data = self.storage_adapter.read_data(
            file_name_with_path=file_name_with_path)
        data = pd.read_csv(StringIO(raw_data), dtype=str, on_bad_lines="warn")

        # Process orders and handle errors
        data.apply(self.process_order, axis=1)

        # Aggregate sales summary
        summary_df = data.groupby(["CustomerID", "ProductID"]).agg(
            {"TotalAmount": "sum"}).reset_index()

        # Process sales summary and handle errors
        summary_df.apply(self.process_sales_summary, axis=1)

        # Save error log if there are any errors
        error_file_name = f"error_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.save_error_log(error_file_name)

        # Delete original data file after processing
        self.storage_adapter.delete_data(
            file_name_with_path=file_name_with_path)

    def process_order(self, row):
        try:
            order_data = {
                "OrderID": row["OrderID"],
                "OrderDate": row["OrderDate"],
                "CustomerID": row["CustomerID"],
                "ProductID": row["ProductID"],
                "Quantity": row["Quantity"],
                "UnitPrice": row["UnitPrice"],
                "TotalAmount": row["TotalAmount"],
            }
            self.db_adapter.insert_order(order_data)
        except Exception as e:
            # Log the error and the row that caused it
            self.log_error(row.to_dict(), str(e))

    def process_sales_summary(self, row):
        try:
            summary_data = {
                "CustomerID": row["CustomerID"],
                "ProductID": row["ProductID"],
                "TotalSales": row["TotalAmount"],
            }
            self.db_adapter.insert_sales_summary(summary_data)
        except Exception as e:
            # Log the error and the row that caused it
            self.log_error(row.to_dict(), str(e))
