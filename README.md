# Stocks-Database
Using Python to connect to Database and import csv files to tables
See it implemented at http://citest.clickable.com. 

[Overview]

Download csv files from url via request. Use csv file to create a DataFrame.
created and changed columns and data to fit our needs. Exported dataframe back to csv

BULK INSERT csv into database table.

-------
[Details]

Choose to bulk insert csv into database table instead of dataframe to database
df.to_sql and was only compatible with some Database libraries
BULK INSERT could be used across any Database library and can be much faster than to_sql.

I did not make a scheduler to make it run evreyday at a certain time in python.
If I created one it would be inferior to free third party apps than can run on startup and much more.
For windows there is a default app called task scheduler.
https://www.howtogeek.com/123393/how-to-automatically-run-programs-and-set-reminders-with-the-windows-task-scheduler/

The database connection could be changed as needed in the main(). ex: library and paramaters.

------
[Future Updates]

Some possible things I can add to this program is to add try and catch for error handling.
Minimizing and optimizing database storage and insert.

------
[SQL TABLE]


CREATE TABLE StockCompList5 (

    Ticker varchar(14) NOT NULL,
    CompanyName varchar(200) NOT NULL,
	LastSale float,
	MarketCap varchar(14),
	IPOyear smallint,
	Sector varchar(200),
	Industry varchar(200),
	Market varchar(14)
);
