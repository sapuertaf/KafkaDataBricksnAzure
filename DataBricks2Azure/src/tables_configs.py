columns_types_2_transform:list = ["price", "percentage"]

global_confs = {
    "sales" : {
      "ExtendedAmount":columns_types_2_transform[0],
      "UnitPrice":columns_types_2_transform[0],
      "ProductStandardCost":columns_types_2_transform[0],
      "TotalProductCost":columns_types_2_transform[0],
      "SalesAmount":columns_types_2_transform[0],
      "UnitPriceDiscountPct" :columns_types_2_transform[1],  
    },
    "product" : {
        "StandardCost":columns_types_2_transform[0],
        "ListPrice":columns_types_2_transform[0],
    }
}