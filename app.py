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
SalesByTime1=SalesByTime
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
try:
    SalesByCity = pd.read_sql_query("""
    select
    od.order_id, sum(od.quantity * od.unit_price) as sales, o.ship_city , o.ship_country_region
    from order_details od
    inner join orders o
    on  o.id = od.order_id
    group by o.ship_city, o.ship_country_region ;
    """,connection)
    SalesByCity.to_csv("SalesByCity.csv")
except:
    SalesByCity = pd.read_csv("SalesByCity.csv")
try:
    SalesBySuppliers = pd.read_sql_query("""
    SELECT
    s.company, sum(od.quantity * od.unit_price) as sales
    FROM
    order_details od
    inner join  products  p
    on  p.id = od.product_id
    inner JOIN suppliers s
    on s.id = p.supplier_ids
    group  by  s.company;
""",connection)
    SalesBySuppliers.to_csv("SalesBySuppliers.csv")
except:
    SalesBySuppliers=pd.read_csv("SalesBySuppliers.csv")

try:
    Top10Employees = pd.read_sql_query("""
    SELECT o.employee_id,
    e.first_name, e.last_name,  sum(od.quantity * od.unit_price) as totalsales
    FROM
    order_details od
    inner JOIN orders o
    on od.order_id = o.id
    inner join employees e
    on o.employee_id = e.id
    group by
    o.employee_id
    order by sum(od.quantity * od.unit_price) DESC limit 10;
""",connection)
    Top10Employees.to_csv("Top10Employees.csv")
except:
    Top10Employees=pd.read_csv("Top10Employees.csv")
try:
    Shipper = pd.read_sql_query("""
    SELECT 
    s.company , sum(od.quantity) as quantity
    FROM
    order_details od
    inner JOIN orders o
    on od.order_id = o.id
    inner join shippers s
    on o.shipper_id = s.id
    group by
    s.id
    order by sum(od.quantity) DESC ;
    """,connection)
    Shipper.to_csv("Shipper.csv")
except:
    Shipper=pd.read_csv("Shipper.csv")
try:
    LatestOrders = pd.read_sql_query("""
    SELECT  O.order_date, P.product_name,
    OD.quantity, 
    OD.unit_price, 
    OD.quantity * OD.unit_price  AS Price, ods.status_name
    FROM
    orders o
    INNER JOIN order_details od
    ON o.id = od.order_id
    INNER JOIN
    order_details_status  ods
    ON  od.status_id = ods.id
    INNER JOIN
    products p
    ON  od.product_id = p.id
    order by o.order_date
    DESC LIMIT 5;
    """,connection)
    LatestOrders.to_csv("LatestOrders.csv")
except:
    LatestOrders=pd.read_csv("LatestOrders.csv")
try:
    SalesByCatagory = pd.read_sql_query("""
SELECT p.category,OD.quantity*OD.unit_price as Sales FROM orders o
INNER JOIN order_details od
ON 	o.id=od.order_id
INNER JOIN order_details_status ods
ON od.status_id =ods.id
INNER JOIN products p
ON od.product_id=p.id
GROUP By p.category
order by OD.quantity*OD.unit_price DESC;
 """,connection)
    SalesByCatagory.to_csv("SalesByCatagory.csv")
except:
    SalesByCatagory=pd.read_csv("SalesByCatagory.csv")
app = Flask(__name__)


@app.route("/",methods=["GET","POST"])
def chart():
    Company = list(salesBycompanies["Company"])
    TotalSales = list(salesBycompanies["Total"])
    Product_Name = list(Top10Products["Product_Name"])
    Total_Units_Sold = list(Top10Products["Total Units Sold"])
    Shipped_Date = [str(i) for i in SalesByTime["Shipped_Date"]]
    TotalSaless = [float(i.replace(",","")) for i in SalesByTime["Total"] ]
    Monthlysalestotal = [float(i) for i in SalesByMonth["total"]]
    MonthlysalesMonths = list(SalesByMonth["month"])
    Yearsalestotal = [float(i) for i in SalesByYear["total"]]
    Yearsales = list(SalesByYear["year"])
    TotalAllSales=sum(Yearsalestotal)


    mydate = datetime.datetime.now()
    mydate.strftime("%A")  # 'December'
    ThisYear = mydate.strftime("%Y")
    ThisMonth = mydate.strftime("%B")
    df = SalesByTime
    try:

        df['Qtr'] = df['Shipped_Date'].apply(lambda x: x.strftime('%m'))
        df['Qtr'] = pd.to_numeric(df['Qtr']) // 4 + 1
        df['Year'] = df['Shipped_Date'].apply(lambda x: x.strftime('%Y'))
        df['Qtr_Yr'] = df['Year'].astype(str) + '-Q' + df['Qtr'].astype(str)
        ThisMonthTotalSales = df[(df["Month"] == ThisMonth) & (df["Year"] == "ThisYear")]["Total"].sum()
    except:
        df['Shipped_Date'] = [datetime.datetime.strptime(i, '%Y-%m-%d') for i in df['Shipped_Date']]
        df['Qtr'] = df['Shipped_Date'].apply(lambda x: x.strftime('%m'))
        df['Qtr'] = pd.to_numeric(df['Qtr']) // 4 + 1
        df['Year'] = df['Shipped_Date'].apply(lambda x: x.strftime('%Y'))
        df['Qtr_Yr'] = df['Year'].astype(str) + '-Q' + df['Qtr'].astype(str)
        ThisMonthTotalSales = df[(df["Month"] == ThisMonth) & (df["Year"] == "ThisYear")]["Total"].sum()
    Suppliers=list(SalesBySuppliers["company"])
    salesbysuppliersale=list(SalesBySuppliers["sales"])
    cities= list(SalesByCity["ship_city"])
    salesCitysales = list(SalesByCity["sales"])
    Top10Employeess=Top10Employees.T
    employee0=list(Top10Employeess[0])
    employee1 = list(Top10Employeess[1])
    employee2 = list(Top10Employeess[2])
    employee3 = list(Top10Employeess[3])
    employee4 = list(Top10Employeess[4])
    ShipperCompany=list(Shipper["company"])
    ShipperQuantity=list(Shipper["quantity"])
    LatestOrderss = LatestOrders.T
    LatestOrder0 = list(LatestOrderss[1])
    LatestOrder1 = list(LatestOrderss[2])
    LatestOrder2 = list(LatestOrderss[3])
    LatestOrder3 = list(LatestOrderss[4])
    LatestOrder4 = list(LatestOrderss[5])
    Catagory = list(SalesByCatagory["category"])
    SalesCatagory = list(SalesByCatagory["Sales"])


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
                           TotalAllSales=TotalAllSales,
                           ThisMonthTotalSales=ThisMonthTotalSales,
                           ThisMonth=ThisMonth,
                           Suppliers=Suppliers,
                           salesbysuppliersale=salesbysuppliersale,
                           cities=cities,
                           salesCitysales=salesCitysales,
                           employee0=employee0,
                           employee1=employee1,
                           employee2=employee2,
                           employee3=employee3,
                           employee4=employee4,
                           ShipperCompany=ShipperCompany,
                           ShipperQuantity=ShipperQuantity,
                           LatestOrder0=LatestOrder0,
                           LatestOrder1=LatestOrder1,
                           LatestOrder2=LatestOrder2,
                           LatestOrder3=LatestOrder3,
                           LatestOrder4=LatestOrder4,
                           Catagory=Catagory,
                           SalesCatagory=SalesCatagory)

if __name__ == "__main__":
    app.debug=True
    app.run()
