
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
1001,2024-08-01,C001,P001,10,15.00,150.00                                             CustomerID 
1002,2024-08-01,C002,P002,5,25.00,125.00
1003,2024-08-02,C001,P003,2,30.00,60.00
...


Data Loading: Trigger Data Loading -> AWS S3 trigger on storage change
> dict Can be very large |  -> to my consumer 
Load the cleaned and transformed data into a relational database (e.g., PostgreSQL). # Should use adapter based code -> default SQLite
Create two tables: Orders and SalesSummary. -> Create if not exists
Orders table should have columns matching the CSV file.
SalesSummary table should contain aggregated sales data with columns CustomerID, ProductID, and TotalSales.
		On success clear cleaned data from storage AWS S3
		


# Overall the script should take a config connect to Queue (Default in-mem | Should use adapter based code supporting connection to RabbitMQ and Kafka)
# -> Start reading from the storage locations from queue using respective storage adapter (default current filesystem) -> and start writing into the DB

# Make this queue thing generic so that it can be reused for transformation 
# So 1 queue to know if an extracted data is ready to transform
# And 1 queue to know if a transformed data is ready to load