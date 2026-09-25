USE master;
GO

CREATE DATABASE order_analytics;
GO

USE order_analytics;
GO

CREATE EXTERNAL DATA SOURCE order_data_lake
WITH (
    LOCATION = 'https://storderlakehbrs160926.dfs.core.windows.net/order-data'
);
GO

CREATE VIEW dbo.vw_daily_order_metrics
AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/daily_order_metrics/',
    DATA_SOURCE = 'order_data_lake',
    FORMAT = 'DELTA'
) AS gold;
GO
