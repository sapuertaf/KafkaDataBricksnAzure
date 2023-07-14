from pyspark.sql.types import StructType, IntegerType,StringType,FloatType

schema_customer = StructType()\
                  .add("CustomerKey",IntegerType())\
                  .add("CustomerID",StringType())\
                  .add("Customer",StringType())\
                  .add("City",StringType())\
                  .add("StateProvince",StringType())\
                  .add("CountryRegion",StringType())\
                  .add("PostalCode",StringType())


schema_date = StructType()\
              .add("DateKey",IntegerType())\
              .add("Date",StringType())\
              .add("FiscalYear",StringType())\
              .add("FiscalQuarter",StringType())\
              .add("Month",StringType())\
              .add("FullDate",StringType())\
              .add("MonthKey",IntegerType())

schema_product = StructType()\
                 .add("ProductKey",IntegerType())\
                 .add("SKU",StringType())\
                 .add("Product",StringType())\
                 .add("StandardCost",FloatType())\
                 .add("Color",StringType())\
                 .add("ListPrice",FloatType())\
                 .add("Model",StringType())\
                 .add("Subcategory",StringType())\
                 .add("Category",StringType())

schema_reseller = StructType()\
                 .add("ResellerKey",IntegerType())\
                 .add("ResellerID",StringType())\
                 .add("BusinessType",StringType())\
                 .add("Reseller",StringType())\
                 .add("City",StringType())\
                 .add("StateProvince",StringType())\
                 .add("CountryRegion",StringType())\
                 .add("PostalCode",StringType())

schema_sales_order = StructType()\
                 .add("Channel",StringType())\
                 .add("SalesOrderLineKey",IntegerType())\
                 .add("SalesOrder",StringType())\
                 .add("SalesOrderLine",IntegerType())

schema_sales_territory = StructType()\
                         .add("SalesTerritoryKey",IntegerType())\
                         .add("Region",StringType())\
                         .add("Country",StringType())\
                         .add("Group",StringType())