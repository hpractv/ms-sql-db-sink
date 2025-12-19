@echo off
REM Quick Start Script for SQL Server Sink
REM Usage: run-sync.bat

echo SQL Server Sink - Database Record Sync Tool
echo ============================================
echo.

REM Check if args are provided
if "%~1"=="" (
    echo Usage: run-sync.bat ^<sourceServer^> ^<sourceDb^> ^<targetServer^> ^<targetDb^> [tableName] [options]
    echo.
    echo Example:
    echo   run-sync.bat "source..." "SourceDB" "target..." "TargetDB" "dbo.Users" --batch-size 1000
    echo.
    echo   To sync multiple tables/schemas:
    echo   run-sync.bat "source..." "SourceDB" "target..." "TargetDB" "dbo.Users, Sales, HR.Employees"
    echo.
    echo   To sync ALL tables, use "all" or "*" as tableName:
    echo   run-sync.bat "source..." "SourceDB" "target..." "TargetDB" "all"
    echo.
    echo   Use --help for all available options
    echo.
    exit /b 1
)

REM Run the application
dotnet run -- %*
