from flask import Flask
from flask import render_template
import datetime
import os
import numpy as np
import pandas as pd
import mysql.connector


try:
    connection = mysql.connector.connect(host="localhost",
                                   user  ="root",
                                     passwd="",
                                   db="northwind",
                                     auth_plugin='mysql_native_password')

    tables=pd.read_sql_query("SHOW TABLES FROM northwind",connection)

except:
    pass
# Chart1 """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
try:
    salesBycompanies=pd.read_sql_query("""
    SELECT c.Company,
    SUM((od.Quantity * od.Unit_Price)) AS Total
    FROM Customers c
    INNER JOIN Orders o
    ON c.ID = o.Customer_ID
    inner join
    `Order_Details` od
    ON o.ID = od.Order_ID
    INNER JOIN
    Products p
    ON od.Product_ID = p.ID
    GROUP BY  c.Company
    ORDER BY Total DESC;
    
    """,connection)
    salesBycompanies.to_csv("salesBycompanies.csv")
except:
    salesBycompanies=pd.read_csv("salesBycompanies.csv")

try:
    Top10Products = pd.read_sql_query("""
    SELECT p.Product_Name, SUM(od.Quantity) AS `Total Units Sold`
    FROM
    Orders o
    inner join
    `Order_Details` od
    ON o.ID = od.Order_ID
    INNER JOIN
    Products p
    ON od.Product_ID = p.ID
    GROUP BY p.Product_Name
    ORDER BY `Total Units Sold` DESC LIMIT 5;
    
    
    """,connection)
    Top10Products.to_csv("Top10Products.csv")
except:
    Top10Products=pd.read_csv("Top10Products.csv")

try:
    SalesByTime = pd.read_sql_query("""
    
    select distinct date(o.Shipped_Date) as Shipped_Date, 
        o.ID, 
        od.Total, 
        monthname(o.Shipped_Date) as Month
    from Orders o
    inner join
    ( select distinct Order_ID, 
            format(sum(Unit_Price * Quantity * (1 - Discount)), 2) as total
        from `order_details`
        group by Order_ID    
    ) od on o.ID = od.Order_ID
    where o.Shipped_Date is not null
        and o.Shipped_Date between date('2000-12-24') and date('2008-09-30')
    order by Shipped_Date;
    
    
    """,connection)
    SalesByTime.to_csv("SalesByTime.csv")
except:
    SalesByTime=pd.read_csv("SalesByTime.csv")
try:

    SalesByMonth = pd.read_sql_query("""
    select month,sum(total) as total
    from (
    select distinct date(o.Shipped_Date) as Shipped_Date, 
        o.ID, 
        od.Total, 
        monthname(o.Shipped_Date) as Month
    from Orders o
    inner join
    ( select distinct Order_ID, 
            format(sum(Unit_Price * Quantity * (1 - Discount)), 2) as total
        from `order_details`
        group by Order_ID    
    ) od on o.ID = od.Order_ID
    where o.Shipped_Date is not null
        and o.Shipped_Date between date('2000-12-24') and date('2008-09-30')
    order by Shipped_Date) t
    group by month
    ;
    """,connection)
    SalesByMonth.to_csv("SalesByMonth.csv")
except:
    SalesByMonth=pd.read_csv("SalesByMonth.csv")


try:
    SalesByYear = pd.read_sql_query("""
    select year,sum(total) as total
    from (
    select distinct date(o.Shipped_Date) as Shipped_Date, 
        o.ID, 
        od.Total, 
        year(o.Shipped_Date) as year
    from Orders o
    inner join
    ( select distinct Order_ID, 
            format(sum(Unit_Price * Quantity * (1 - Discount)), 2) as total
        from `order_details`
        group by Order_ID    
    ) od on o.ID = od.Order_ID
    where o.Shipped_Date is not null
        and o.Shipped_Date between date('2000-12-24') and date('2008-09-30')
    order by Shipped_Date) t
    group by year
    ;
    """,connection)
    SalesByYear.to_csv("SalesByYear.csv")
except:
    SalesByYear=pd.read_csv("SalesByYear.csv")
try:

    ProductsAboveAveragePrice = pd.read_sql_query("""
    SELECT Products.Product_Name ,
    Products.list_Price 
    FROM Products 
    WHERE (Products.list_Price) > ( SELECT AVG(list_Price) From Products ) 
    ORDER BY Products.list_Price DESC
    """,connection)
except:
    pass
app = Flask(__name__)


@app.route("/",methods=["GET","POST"])
def chart():
    Company = list(salesBycompanies["Company"])
    TotalSales = list(salesBycompanies["Total"])
    Product_Name = list(Top10Products["Product_Name"])
    Total_Units_Sold = list(Top10Products["Total Units Sold"])
    Shipped_Date = [str(i) for i in SalesByTime["Shipped_Date"]]
    TotalSaless = [float(str(i).replace(",","")) for i in SalesByTime["Total"] ]
    Monthlysalestotal = [float(i) for i in SalesByMonth["total"]]
    MonthlysalesMonths = list(SalesByMonth["month"])
    Yearsalestotal = [float(i) for i in SalesByYear["total"]]
    Yearsales = list(SalesByYear["year"])
    TotalAllSales=sum(Yearsalestotal)
    SalesByTime["Total"]=[float(str(i).replace(",","")) for i in SalesByTime["Total"] ]

    mydate = datetime.datetime.now()
    mydate.strftime("%A")  # 'December'
    ThisYear = mydate.strftime("%Y")
    ThisMonth = mydate.strftime("%B")
    df = SalesByTime
    df['Qtr'] = df['Shipped_Date'].apply(lambda x: x.strftime('%m'))
    df['Qtr'] = pd.to_numeric(df['Qtr']) // 4 + 1
    df['Year'] = df['Shipped_Date'].apply(lambda x: x.strftime('%Y'))
    df['Qtr_Yr'] = df['Year'].astype(str) + '-Q' + df['Qtr'].astype(str)
    ThisMonthTotalSales = df[(df["Month"] == ThisMonth) & (df["Year"] == "ThisYear")]["Total"].sum()





    return render_template('index.html',
                           Company=Company,
                           TotalSales=TotalSales ,
                           Product_Name=Product_Name,
                           Total_Units_Sold=Total_Units_Sold,
                           Shipped_Date=Shipped_Date,
                           TotalSaless=TotalSaless,
                           Monthlysalestotal=Monthlysalestotal,
                           MonthlysalesMonths=MonthlysalesMonths,
                           Yearsalestotal=Yearsalestotal,
                           Yearsales=Yearsales,
                           TotalAllSales=round(TotalAllSales),
                           ThisMonthTotalSales=ThisMonthTotalSales,
                           ThisMonth=ThisMonth)




if __name__ == "__main__":
    app.run(debug=True)
