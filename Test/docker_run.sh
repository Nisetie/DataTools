#!/bin/bash
sudo docker run -d -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=1qaz@WSX -p 1433:1433 --rm mcr.microsoft.com/mssql/server;
sudo docker run -d -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=1qaz@WSX -p 1440:1433 --rm mcr.microsoft.com/mssql/server;
sudo docker run --name postgres1 -d -p 5432:5432 -e POSTGRES_PASSWORD=1qaz@WSX -e POSTGRES_DB=test --rm postgres;
sudo docker run --name postgres2 -d -p 5433:5432 -e POSTGRES_PASSWORD=1qaz@WSX -e POSTGRES_DB=test1 --rm postgres;