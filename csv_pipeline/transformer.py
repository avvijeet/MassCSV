import pandas as pd

from sanctify import Cleanser, Transformer, process_cleansed_df, PrimitiveDataTypes, Constants, DateOrderTuples


from enum import StrEnum, auto


# Raw Dataset:
# Let's say you have a CSV file named sales_data.csv with the following columns:
# OrderID: Unique identifier for each order
# OrderDate: Date when the order was placed
# CustomerID: Unique identifier for the customer
# ProductID: Unique identifier for the product
# Quantity: Number of items ordered
# UnitPrice: Price per item
# TotalAmount: Total amount of the order (Quantity * UnitPrice)
 
# Example of the raw data:
 
# OrderID,OrderDate,CustomerID,ProductID,Quantity,UnitPrice,TotalAmount ERROR
# 1001,2024-08-01,C001,P001,10,15.00,150.00                                             CustomerID 
# 1002,2024-08-01,C002,P002,5,25.00,125.00
# 1003,2024-08-02,C001,P003,2,30.00,60.00


class MyCustomCleanser(Cleanser):
    def validate_total_amount(self):
        """Calculate the TotalAmount if it's missing or incorrect (i.e., Quantity * UnitPrice)."""
        pass

    def aggregate_total_sales_per_customer_and_product_id(self):
        """Aggregate the total sales per CustomerID and ProductID."""
        pass

class MyCustomDataTypes(StrEnum):
    # OrderID = auto()
    # OrderDate= auto()
    # CustomerID= auto()
    # ProductID= auto()
    # Quantity= auto()
    # UnitPrice= auto()
    TotalAmount= auto()
    Date = auto()
    Amount = auto()

class MyStandardColumns(StrEnum):
    OrderID = auto()
    OrderDate= auto()
    CustomerID= auto()
    ProductID= auto()
    Quantity= auto()
    UnitPrice= auto()
    TotalAmount= auto()

    def _generate_next_value_(name, start, count, last_values): # NOTE: 'self' not needed
        return name



# Add custom data type on change
DATA_TYPE_SCHEMA = {
    MyCustomDataTypes.Date.value: {
        Constants.TRANSFORMATIONS.value: [
            (
                Transformer.parse_date_from_string,
                {
                    Constants.DATE_ORDER_TUPLE.value: DateOrderTuples.YEAR_MONTH_DAY.value
                },
            )
        ],
    },
    MyCustomDataTypes.Amount.value: {
        Constants.TRANSFORMATIONS.value: [
            Transformer.remove_currency_from_amount,
        ],
    },
}

# AND use the Data Types Defined above in your column mapping
# The below dictionary represents the mapping of the Input Column Vs the Standard Column in your system with its Data type
COLUMN_MAPPING = {  # NOTE: Make sure that this doesn't change during processing
    "OrderID": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.OrderID.value,
        Constants.DATA_TYPE.value: PrimitiveDataTypes.STRING.value, # Keeping it as is
    },
    "OrderDate": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.OrderDate.value,
        Constants.DATA_TYPE.value: MyCustomDataTypes.Date.value, # Custom data type handles all types of valid date strings
    },
    "CustomerID": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.CustomerID.value,
        Constants.DATA_TYPE.value: PrimitiveDataTypes.STRING.value, # Keeping it as is
    },
    "ProductID": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.ProductID.value,
        Constants.DATA_TYPE.value: PrimitiveDataTypes.STRING.value, # Keeping it as is
    },
    "Quantity": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.Quantity.value,
        Constants.DATA_TYPE.value: PrimitiveDataTypes.FLOAT.value, # Can change from int to float anytime
    },
    "UnitPrice": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.UnitPrice.value,
        Constants.DATA_TYPE.value: MyCustomDataTypes.Amount.value, # Custom data type handles currency
    },
    "TotalAmount": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.TotalAmount.value,
        Constants.DATA_TYPE.value: MyCustomDataTypes.Amount.value, # Custom data type handles currency
    },
}


def cleanse_and_validate(
    input_file_path: str,
    cleansed_processed_output_file_path: str,
) -> None | Exception:
    """
        Raw Dataset:
        Let's say you have a CSV file named sales_data.csv with the following columns:
        OrderID: Unique identifier for each order
        OrderDate: Date when the order was placed
        CustomerID: Unique identifier for the customer
        ProductID: Unique identifier for the product
        Quantity: Number of items ordered
        UnitPrice: Price per item
        TotalAmount: Total amount of the order (Quantity * UnitPrice)
        
        Example of the raw data:
        
        OrderID,OrderDate,CustomerID,ProductID,Quantity,UnitPrice,TotalAmount ERROR
        1001,2024-08-01,C001,P001,10,15.00,150.00                                        
        1002,2024-08-01,C002,P002,5,25.00,125.00
        1003,2024-08-02,C001,P003,2,30.00,60.00
    """

    # Step 2: Read the CSV data
    input_df = pd.read_csv(input_file_path, dtype=str)

    # Step 3: Perform cleansing operations
    cleanser = Cleanser(df=input_df, column_mapping_schema=COLUMN_MAPPING, data_type_schema=DATA_TYPE_SCHEMA)
    _ = cleanser.remove_trailing_spaces_from_column_headers()
    # _ = cleanser.drop_unmapped_columns()
    _ = cleanser.drop_fully_empty_rows()
    _ = cleanser.remove_trailing_spaces_from_each_cell_value()
    _, updated_column_mapping_schema = cleanser.replace_column_headers()

    # Step 4: Run Transformations and Validations as defined in the column mapping above
    # NOTE: This step adds the column 'Error' into the df
    processed_df = process_cleansed_df(
        df=cleanser.df,
        column_mapping_schema=updated_column_mapping_schema,
        data_type_schema=DATA_TYPE_SCHEMA,
        ignore_optional_columns=False,
    )

    # Optional Step 5: Drop all rows that have the Error column populated or in essence which failed validations
    # _ = cleanser.modify_error_column_to_set_all_except_mandatory_to_blank()
    # _ = cleanser.drop_rows_with_errors(inplace=True) # TODO: Handled errored rows

    # Alternatively
    # ignore_columns_list = cleanser.get_optional_column_names_from_column_mapping()
    # cleanser.drop_rows_with_errors(inplace=True, ignore_columns_list=ignore_columns_list)

    # Optional Step 7: Extract the final df as csv
    processed_df.to_csv(cleansed_processed_output_file_path, index=False)


if __name__ == "__main__":
    # Step 1: Define file paths
    input_file_path = "<path to>/input.csv"
    cleansed_processed_output_file_path = "<path to>/CLEANSED_PROCESSED_input.csv"

    cleanse_and_validate(
        input_file_path=input_file_path,
        cleansed_processed_output_file_path=cleansed_processed_output_file_path,
    )