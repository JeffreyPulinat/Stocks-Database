import requests
import pandas as pd
import pypyodbc
import numpy as np
import os

def download_csv(market_name, url):
    """
    Downloads csv from given url to be placed in current directory
    :param market_name: datafile name
    :param url: url link
    """
    r = requests.get(url)
    with open(market_name+'.csv', 'wb') as f:
        f.write(r.content)


def csv_to_df(market_name):
    """
    Makes csv into df and throws out unwanted columns
    :param market_name: stock markets name
    :return: datfile without column unnamed and summary quote 
    """
    df = pd.read_csv(market_name + '.csv', index_col=0)
    del df['Summary Quote']     #delete Summary Quote column
    del df['Unnamed: 8']        #delete Unnamed column
    df['Market'] = market_name      #add column market all rows = df_name
    return df


def join_df(df1, df2, df3):
    """
    Join 3 tables by append. All tables have same header.
    :param df1:  ex: NASDAQ datafile
    :param df2:  ex: AMSE datafile
    :param df3:  ex: NYSE datafile
    :return: appended datafile. contains NASDAQ,AMSE,NYSE
    """
    result = df1.append(df2.append(df3))
    return result


def filter_df(df):
    """
    Any Changes to data values take place in this method
    :param df: original datafile 
    :return: changed datafile
    """
    df=df.replace('n/a', np.nan) #replaces n/a with true NULL value
    return df


def csv_to_DB(con, DatabaseName, TableName, CSVname):
    """
    Bulk Inserts csv into SQL table
    :param con:  connection object to database
    :param DatabaseName: ex:DailyStocks
    :param TableName:  ex: AllStockComp
    :param CSVname:    ex: AllStockComp.csv 
    """
    path = os.path.abspath(CSVname)
    path = path.replace('\\','\\\\') #replace backslash with double backslash
    cur = con.cursor()
    querystring = "DELETE FROM "+ TableName +" ;"       #delete all data in table
    cur.execute(querystring)
    querystring = """
                    BULK INSERT """+ DatabaseName + """.dbo.""" + TableName+""" """"""
                    FROM '"""+ path +"""' """""" 
                    WITH(FIRSTROW = 2, FIELDTERMINATOR='|',ROWTERMINATOR='\\n');
                 """        #BULK INSERT fro csv to database table
    #Example Query
    # BULK INSERT DailyStocks.dbo.StockCompList5
    # FROM 'C:\\Users\\User Name\\IdeaProjects\\DailyStocks Database Update v1\\StockCompList.csv'
    # WITH(FIRSTROW = 2, FIELDTERMINATOR='|',ROWTERMINATOR='\\n');
    cur.execute(querystring)
    con.commit()        #save changes


def main():
    url = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
    stock = 'NASDAQ'
    download_csv(stock, url)
    dfNASDAQ = csv_to_df(stock)

    url = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'
    stock = 'NYSE'
    download_csv(stock, url)
    dfNYSE = csv_to_df(stock)

    url = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download'
    stock = 'AMSE'
    download_csv(stock, url)
    dfAMEX = csv_to_df(stock)

    final=join_df(dfNASDAQ, dfNYSE, dfAMEX)
    final = filter_df(final)

    CSVname = 'StockCompList.csv'
    final.to_csv(CSVname, sep = '|') #datfile to csv. New seperator is needed because some values contain commas

    #libray and parameters might need to be changed. This works for my local db.
    con = pypyodbc.connect("Driver={SQL Server};Server=HP\JPULINAT;Database=DailyStocks;Trusted_Connection = Yes")
    DatabaseName = 'DailyStocks'
    TableName = 'StockCompList5'
    csv_to_DB(con, DatabaseName, TableName, CSVname)


main()