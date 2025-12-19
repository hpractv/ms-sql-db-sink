@echo off
REM Setup environment variables for database connections
REM Copy this file to setup-env.bat and fill in your actual values

REM Source Database Details
set SOURCE_SERVER=your-source-server.database.windows.net
set SOURCE_DB=SourceDB

REM Target Database Details
set TARGET_SERVER=your-target-server.database.windows.net
set TARGET_DB=TargetDB

echo Environment variables set!
echo.
echo To run the sync:
echo   dotnet run -- "%SOURCE_SERVER%" "%SOURCE_DB%" "%TARGET_SERVER%" "%TARGET_DB%" [tableName] [options]
echo.
echo Example (single table):
echo   dotnet run -- "%SOURCE_SERVER%" "%SOURCE_DB%" "%TARGET_SERVER%" "%TARGET_DB%" "dbo.Users" --batch-size 1000
echo.
echo Example (multiple tables/schemas):
echo   dotnet run -- "%SOURCE_SERVER%" "%SOURCE_DB%" "%TARGET_SERVER%" "%TARGET_DB%" "dbo.Users, Sales"
echo.
echo Example (all tables):
echo   dotnet run -- "%SOURCE_SERVER%" "%SOURCE_DB%" "%TARGET_SERVER%" "%TARGET_DB%" "all"
echo.
echo Use --help for all available options
