# External Imports
from enum import StrEnum, auto

import pandas as pd
from sanctify import Cleanser, Constants, DateOrderTuples, PrimitiveDataTypes, Transformer, process_cleansed_df


class MyCustomCleanser(Cleanser):
    def validate_total_amount(self):
        """Calculate the TotalAmount if it's missing or incorrect (i.e., Quantity * UnitPrice)."""

        # Use df apply to validate if the Total can be calculated and if yes and if calculation don't match then append 'Error' column with the error and replace value with calculated value
        def validate_and_correct(row):
            calculated_total = row["Quantity"] * row["UnitPrice"]
            if pd.isna(row["TotalAmount"]) or row["TotalAmount"] != calculated_total:
                row["Error"] = f"Incorrect TotalAmount. Expected {calculated_total}, got {row['TotalAmount']}."
                row["TotalAmount"] = calculated_total
            return row

        # Apply the validation and correction
        self.df = self.df.apply(validate_and_correct, axis=1)


class MyCustomDataTypes(StrEnum):
    # OrderID = auto()
    # OrderDate= auto()
    # CustomerID= auto()
    # ProductID= auto()
    # Quantity= auto()
    # UnitPrice= auto()
    TotalAmount = auto()
    Date = auto()
    Amount = auto()


class MyStandardColumns(StrEnum):
    # NOTE: 'self' not needed, _generate_next_value_ must be defined before members
    def _generate_next_value_(name, start, count, last_values):
        return name

    OrderID = auto()
    OrderDate = auto()
    CustomerID = auto()
    ProductID = auto()
    Quantity = auto()
    UnitPrice = auto()
    TotalAmount = auto()


# Add custom data type on change
DATA_TYPE_SCHEMA = {
    MyCustomDataTypes.Date.value: {
        Constants.TRANSFORMATIONS.value: [
            (
                Transformer.parse_date_from_string,
                {Constants.DATE_ORDER_TUPLE.value: DateOrderTuples.YEAR_MONTH_DAY.value},
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
        Constants.DATA_TYPE.value: PrimitiveDataTypes.STRING.value,  # Keeping it as is
    },
    "OrderDate": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.OrderDate.value,
        # Custom data type handles all types of valid date strings
        Constants.DATA_TYPE.value: MyCustomDataTypes.Date.value,
    },
    "CustomerID": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.CustomerID.value,
        Constants.DATA_TYPE.value: PrimitiveDataTypes.STRING.value,  # Keeping it as is
    },
    "ProductID": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.ProductID.value,
        Constants.DATA_TYPE.value: PrimitiveDataTypes.STRING.value,  # Keeping it as is
    },
    "Quantity": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.Quantity.value,
        # Can change from int to float anytime
        Constants.DATA_TYPE.value: PrimitiveDataTypes.FLOAT.value,
    },
    "UnitPrice": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.UnitPrice.value,
        # Custom data type handles currency
        Constants.DATA_TYPE.value: MyCustomDataTypes.Amount.value,
        Constants.POST_PROCESSING_DATA_TYPE: PrimitiveDataTypes.FLOAT.value,
    },
    "TotalAmount": {
        Constants.STANDARD_COLUMN.value: MyStandardColumns.TotalAmount.value,
        # Custom data type handles currency returns string
        Constants.DATA_TYPE.value: MyCustomDataTypes.Amount.value,
        # Optional so that errors logged are ignored in final step
        Constants.IS_OPTIONAL: True,
        Constants.POST_PROCESSING_DATA_TYPE: PrimitiveDataTypes.FLOAT.value,
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
    cleanser = MyCustomCleanser(
        df=input_df, column_mapping_schema=COLUMN_MAPPING, data_type_schema=DATA_TYPE_SCHEMA)
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

    post_processing_cleanser = MyCustomCleanser(
        df=processed_df, column_mapping_schema=updated_column_mapping_schema, data_type_schema=DATA_TYPE_SCHEMA
    )
    post_processing_cleanser.validate_total_amount()

    # Optional Step 5: Drop all rows that have the Error column populated or in essence which failed validations
    # _ = cleanser.modify_error_column_to_set_all_except_mandatory_to_blank()
    # _ = cleanser.drop_rows_with_errors(inplace=True) # TODO: Handled errored rows

    # Alternatively
    # ignore_columns_list = cleanser.get_optional_column_names_from_column_mapping()
    # cleanser.drop_rows_with_errors(inplace=True, ignore_columns_list=ignore_columns_list)

    # Optional Step 7: Extract the final df as csv
    post_processing_cleanser.df.to_csv(
        cleansed_processed_output_file_path, index=False)


if __name__ == "__main__":
    # Step 1: Define file paths
    input_file_path = "<path to>/input.csv"
    cleansed_processed_output_file_path = "<path to>/CLEANSED_PROCESSED_input.csv"

    cleanse_and_validate(
        input_file_path=input_file_path,
        cleansed_processed_output_file_path=cleansed_processed_output_file_path,
    )
