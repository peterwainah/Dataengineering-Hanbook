**CASE**:AN ETL task for querying data from a WEB API,if the query is successful,the
the data will be received back from the API' server.Data received back will be in JSON format.Since data is a dictionary of strings this means we 
must transform it before storing or loading into a database.Also convert of temperature to Celsius