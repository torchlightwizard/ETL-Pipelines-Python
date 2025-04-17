schema_data = [
    {
        "table_name": "polygon_data",
        "columns": {
            "id": "INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
            "after_hours": "DOUBLE",
            "close": "DOUBLE",
            "from": "DATE",
            "high": "DOUBLE",
            "low": "DOUBLE",
            "open": "DOUBLE",
            "pre_market": "DOUBLE",
            "status": "TINYTEXT",
            "symbol": "TINYTEXT",
            "volume": "DOUBLE",
            "otc": "DOUBLE"
        }
    }
]



schema_predict = [
    {
        "table_name": "polygon_predict",
        "columns": {
            "id": "INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
            "date": "DATE",
            "close": "DOUBLE"
        }
    }
]