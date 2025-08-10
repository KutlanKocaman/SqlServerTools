/*

This script contains tests for the procedure GrantPermissionsToProcedure,
defined here:
	

Before running this script, make sure to:
1. Run the script above to create the GrantPermissionsToProcedure procedure.
2. Install the tSQLt framework in the UtilityDb database.

GrantPermissionsToProcedure and its tests are to be run by a member of the sysadmin role.

These tests create various database objects, including databases and logins.
All objects created are either rolled back or deleted afterwards.

Do not run this script on a Production database.

*/

USE UtilityDb;
GO

-- Create the test class for GrantPermissionsToProcedure tests
EXEC tSQLt.NewTestClass @ClassName = N'GrantPermissionsToProcedureTests';

GO

-- Create a stub GrantPermissionsToProcedure_TempTestProcedure
-- to prevent warnings about it not existing
CREATE PROCEDURE GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure
AS
RETURN;

GO

-- Common setup for all tests
CREATE PROCEDURE GrantPermissionsToProcedureTests.SetUp
AS
BEGIN;
	CREATE LOGIN GrantPermissionsToProcedure_TempTestLogin
	WITH PASSWORD = N'TestPassword123!';

	CREATE USER GrantPermissionsToProcedure_TempTestUser
	FOR LOGIN GrantPermissionsToProcedure_TempTestLogin;
	
	EXECUTE sys.sp_executesql @stmt = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure
		AS
		RETURN;';
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test a hash is used in the cert name if the fully qualified name is too long]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @LongStoredProcedureName1 SYSNAME =
		N'ReallyReallyReallyReallyReallyReallyReallyReallyQuiteLongStoredProcedureName1';
	DECLARE @LongStoredProcedureName2 SYSNAME =
		N'ReallyReallyReallyReallyReallyReallyReallyReallyQuiteLongStoredProcedureName2';
	DECLARE @FullyQualifiedProcedureHash1 NVARCHAR(64);
	DECLARE @FullyQualifiedProcedureHash2 NVARCHAR(64);
	DECLARE @ExpectedCertificateName1 SYSNAME;
	DECLARE @ExpectedCertificateName2 SYSNAME;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Create a procedure with a long name.
	SET @Sql = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.' + @LongStoredProcedureName1 + N'
		AS
		BEGIN;

		DECLARE @Variable INT;
		
		END;
	';

	EXEC sys.sp_executesql
		@stmt = @Sql;

	-- Create another procedure with a very similar long name.
	SET @Sql = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.' + @LongStoredProcedureName2 + N'
		AS
		BEGIN;

		DECLARE @Variable INT;
		
		END;
	';
	
	EXEC sys.sp_executesql
		@stmt = @Sql;

	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	INSERT INTO @GrantList
	(
		GrantScope
		, GrantType
		, DatabaseName
		, Permission
	)
	SELECT GrantScope = 'D'
		, GrantType = 'G'
		, DatabaseName = N'UtilityDb'
		, Permission = N'VIEW DATABASE STATE';

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = @LongStoredProcedureName1
		, @GrantList = @GrantList;
		
	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = @LongStoredProcedureName2
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	SET @FullyQualifiedProcedureHash1 =
		CONVERT(NVARCHAR(64)
			, HASHBYTES('SHA2_256'
				, N'UtilityDb.GrantPermissionsToProcedureTests.' + @LongStoredProcedureName1)
			, 2);

	SET @ExpectedCertificateName1 =
		LEFT('Cert for UtilityDb.GrantPermissionsToProcedureTests.' + @LongStoredProcedureName1
			, 64) +
		@FullyQualifiedProcedureHash1;
		
	SET @FullyQualifiedProcedureHash2 =
		CONVERT(NVARCHAR(64)
			, HASHBYTES('SHA2_256'
				, N'UtilityDb.GrantPermissionsToProcedureTests.' + @LongStoredProcedureName2)
			, 2);
			
	SET @ExpectedCertificateName2 =
		LEFT('Cert for UtilityDb.GrantPermissionsToProcedureTests.' + @LongStoredProcedureName2
			, 64) +
		@FullyQualifiedProcedureHash2;
	
	IF NOT EXISTS
	(
		SELECT 1
		FROM sys.certificates
		WHERE name = @ExpectedCertificateName1
	)
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'@ExpectedCertificateName1 not found';
	END;
	
	IF NOT EXISTS
	(
		SELECT 1
		FROM sys.certificates
		WHERE name = @ExpectedCertificateName2
	)
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'@ExpectedCertificateName2 not found';
	END;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test all certificates are dropped if no permissions are granted]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Grant permissions in:
		-- UtilityDb (current database)
		-- msdb (another database)
		-- and at server-level.
	-- This will create certificates in UtilityDb, msdb, and master.
	INSERT INTO @GrantList (GrantScope, GrantType, DatabaseName, Permission)
	VALUES ('D', 'G', N'UtilityDb', N'VIEW DATABASE STATE')
		, ('D', 'G', N'msdb', N'VIEW DATABASE STATE')
		, ('S', 'G', N'', N'VIEW SERVER STATE');

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = N'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;

	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	-- Remove all permissions from the procedure.

	DELETE FROM @GrantList;

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = 'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	IF EXISTS
	(
		
		SELECT 1
		FROM UtilityDb.sys.certificates
		WHERE name = N'Cert for UtilityDb' +
			N'.GrantPermissionsToProcedureTests' +
			N'.GrantPermissionsToProcedure_TempTestProcedure'
	)
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'Certificate exists in UtilityDb.';
	END;
	
	IF EXISTS
	(
		
		SELECT 1
		FROM msdb.sys.certificates
		WHERE name = N'Cert for UtilityDb' +
			N'.GrantPermissionsToProcedureTests' +
			N'.GrantPermissionsToProcedure_TempTestProcedure'
	)
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'Certificate exists in msdb.';
	END;
	
	IF EXISTS
	(
		
		SELECT 1
		FROM master.sys.certificates
		WHERE name = N'Cert for UtilityDb' +
			N'.GrantPermissionsToProcedureTests' +
			N'.GrantPermissionsToProcedure_TempTestProcedure'
	)
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'Certificate exists in master.';
	END;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test both login and master database user can be created from master certificate]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- GrantPermissionsToProcedure_TempTestProcedure created in Setup proc.
	
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	-- Grant both:
		-- database-scope permissions in master
		-- server-level permissions
	INSERT INTO @GrantList (GrantScope, GrantType, DatabaseName, Permission)
	VALUES ('D', 'G', N'master', N'VIEW DATABASE STATE')
		, ('S', 'G', N'', N'VIEW SERVER STATE');

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = N'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;

	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	IF NOT EXISTS
	(
		SELECT 1
		FROM master.sys.database_principals
		WHERE name = N'Cert for UtilityDb' +
			N'.GrantPermissionsToProcedureTests' +
			N'.GrantPermissionsToProcedure_TempTestProcedure'
	)
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'Master user not found.';
	END;

	IF NOT EXISTS
	(
		SELECT 1
		FROM sys.server_principals
		WHERE name = N'Cert for UtilityDb' +
			N'.GrantPermissionsToProcedureTests' +
			N'.GrantPermissionsToProcedure_TempTestProcedure'
	)
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'Server login not found.';
	END;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test database role membership is granted]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	CREATE ROLE GrantPermissionsToProcedure_TempTestRole;

	EXECUTE sys.sp_executesql @stmt = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure
		AS
		BEGIN;

		IF ISNULL(IS_ROLEMEMBER(''GrantPermissionsToProcedure_TempTestRole''), 0) = 0
			THROW 50000, ''Not a member of GrantPermissionsToProcedure_TempTestRole'', 0;

		END;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	INSERT INTO @GrantList
	(
		GrantScope
		, GrantType
		, DatabaseName
		, Permission
	)
	SELECT GrantScope = 'D'
		, GrantType = 'R'
		, DatabaseName = N'UtilityDb'
		, Permission = N'GrantPermissionsToProcedure_TempTestRole';

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = N'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	EXECUTE AS LOGIN = N'GrantPermissionsToProcedure_TempTestLogin';

	EXEC GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure;

	REVERT;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test database-scoped permissions are granted (VIEW DATABASE STATE)]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create a procedure which SELECTs from sys.dm_db_partition_stats,
	-- which requires VIEW DATABASE STATE permissions.

	EXECUTE sys.sp_executesql @stmt = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure
		AS
		BEGIN;

		DECLARE @Variable INT;

		SELECT @Variable = @Variable
		FROM sys.dm_db_partition_stats;

		END;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	INSERT INTO @GrantList
	(
		GrantScope
		, GrantType
		, DatabaseName
		, Permission
	)
	SELECT GrantScope = 'D'
		, GrantType = 'G'
		, DatabaseName = N'UtilityDb'
		, Permission = N'VIEW DATABASE STATE';

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = N'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	EXECUTE AS LOGIN = N'GrantPermissionsToProcedure_TempTestLogin';

	EXEC GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure;

	REVERT;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test execute as the dbo user to avoid malicious DDL triggers]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;
	
	------------------------------------------------------------
	-- Ensure requirements are met
	------------------------------------------------------------

	IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 0
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'This test requires you to be a sysadmin.';
	END

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	CREATE LOGIN GrantPermissionsToProcedure_TempTestMaliciousDboLogin
	WITH PASSWORD = N'TestPassword123!';

	CREATE USER GrantPermissionsToProcedure_TempTestMaliciousDboUser
	FOR LOGIN GrantPermissionsToProcedure_TempTestMaliciousDboLogin;

	ALTER ROLE db_owner
	ADD MEMBER GrantPermissionsToProcedure_TempTestMaliciousDboUser;

	-- As MaliciousDboLogin, create a DDL trigger to grant themselves sysadmin.
	EXECUTE AS LOGIN = N'GrantPermissionsToProcedure_TempTestMaliciousDboLogin';

	EXEC sys.sp_executesql @stmt = N'
		CREATE OR ALTER TRIGGER MaliciousDdlTrigger
		ON DATABASE
		FOR DDL_DATABASE_LEVEL_EVENTS
		AS
		BEGIN;
		IF IS_SRVROLEMEMBER(N''sysadmin'') = 1
			EXEC sys.sp_executesql @stmt = N''
				USE master ALTER SERVER ROLE sysadmin
				ADD MEMBER GrantPermissionsToProcedure_TempTestMaliciousDboLogin;'';
		END;
	';

	REVERT;
	
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	INSERT INTO @GrantList
	(
		GrantScope
		, GrantType
		, DatabaseName
		, Permission
	)
	SELECT GrantScope = 'D'
		, GrantType = 'G'
		, DatabaseName = N'UtilityDb'
		, Permission = N'VIEW DATABASE STATE';

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = 'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	IF IS_SRVROLEMEMBER(N'sysadmin', N'GrantPermissionsToProcedure_TempTestMaliciousDboLogin') = 1
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'Malicious db_owner was able to make themselves sysadmin.';
	END;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test impersonate user permissions are granted]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	CREATE USER GrantPermissionsToProcedure_TempTestUserToBeImpersonated
	WITHOUT LOGIN;

	EXECUTE sys.sp_executesql @stmt = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure
		AS
		BEGIN;

		EXECUTE AS USER = ''GrantPermissionsToProcedure_TempTestUserToBeImpersonated'';

		END;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	INSERT INTO @GrantList
	(
		GrantScope
		, GrantType
		, DatabaseName
		, Permission
	)
	SELECT GrantScope = 'D'
		, GrantType = 'G'
		, DatabaseName = N'UtilityDb'
		, Permission =
			N'IMPERSONATE ON USER::GrantPermissionsToProcedure_TempTestUserToBeImpersonated';

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = N'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	EXECUTE AS LOGIN = N'GrantPermissionsToProcedure_TempTestLogin';

	EXEC GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure;

	REVERT;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test SELECT permissions are granted to a table in a different database]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE TABLE dbo.GrantPermissionsToProcedure_TempTestTable
		(
			Id INT
		);';

	EXECUTE sys.sp_executesql @stmt = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure
		AS
		BEGIN;

		DECLARE @Variable INT;

		SELECT @Variable = @Variable
		FROM master.dbo.GrantPermissionsToProcedure_TempTestTable;

		END;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	INSERT INTO @GrantList
	(
		GrantScope
		, GrantType
		, DatabaseName
		, Permission
	)
	SELECT GrantScope = 'D'
		, GrantType = 'G'
		, DatabaseName = N'master'
		, Permission = N'SELECT ON dbo.GrantPermissionsToProcedure_TempTestTable';

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = N'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	EXECUTE AS LOGIN = N'GrantPermissionsToProcedure_TempTestLogin';

	EXEC GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure;

	REVERT;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test SELECT permissions are granted to a table in the same DB, with a broken ownership chain]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	CREATE TABLE GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestTable
	(
		Id INT
	);

	-- Break the ownership chain on GrantPermissionsToProcedure_TempTestTable

	CREATE USER GrantPermissionsToProcedure_TempTestTableOwner
	WITHOUT LOGIN;

	ALTER AUTHORIZATION
	ON GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestTable
	TO GrantPermissionsToProcedure_TempTestTableOwner;

	-- Create the procedure to select from GrantPermissionsToProcedure_TempTestTable

	EXECUTE sys.sp_executesql @stmt = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure
		AS
		BEGIN;

		DECLARE @Variable INT;

		SELECT @Variable = @Variable
		FROM GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestTable;

		END;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	INSERT INTO @GrantList
	(
		GrantScope
		, GrantType
		, DatabaseName
		, Permission
	)
	SELECT GrantScope = 'D'
		, GrantType = 'G'
		, DatabaseName = N'UtilityDb'
		, Permission =
			N'SELECT ON GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestTable';

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = N'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	EXECUTE AS LOGIN = N'GrantPermissionsToProcedure_TempTestLogin';

	EXEC GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure;

	REVERT;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test server role membership is granted (bulkadmin)]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	EXECUTE sys.sp_executesql @stmt = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure
		AS
		BEGIN;
		
		IF ISNULL(IS_SRVROLEMEMBER(N''bulkadmin''), 0) = 0
			THROW 50000, N''Not a member of bulkadmin'', 0;

		END;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	INSERT INTO @GrantList
	(
		GrantScope
		, GrantType
		, DatabaseName
		, Permission
	)
	SELECT GrantScope = 'S'
		, GrantType = 'R'
		, DatabaseName = N''
		, Permission = N'bulkadmin';

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = N'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	EXECUTE AS LOGIN = N'GrantPermissionsToProcedure_TempTestLogin';

	EXEC GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure;

	REVERT;
END;
GO

CREATE PROCEDURE
	GrantPermissionsToProcedureTests
	.[test server-scoped permissions are granted (VIEW SERVER STATE)]
AS
BEGIN;
	DECLARE @GrantList dbo.GrantList;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create a procedure whichi SELECTs from sys.dm_xe_sessions,
	-- which requires VIEW SERVER STATE permissions.

	EXECUTE sys.sp_executesql @stmt = N'
		CREATE OR ALTER PROCEDURE
			GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure
		AS
		BEGIN;
		
		DECLARE @Variable INT;

		SELECT @Variable = @Variable
		FROM sys.dm_xe_sessions;

		END;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	INSERT INTO @GrantList
	(
		GrantScope
		, GrantType
		, DatabaseName
		, Permission
	)
	SELECT GrantScope = 'S'
		, GrantType = 'G'
		, DatabaseName = N''
		, Permission = N'VIEW SERVER STATE';

	EXEC UtilityDb.dbo.GrantPermissionsToProcedure
		@ProcedureDatabase = N'UtilityDb'
		, @ProcedureSchema = N'GrantPermissionsToProcedureTests'
		, @ProcedureName = N'GrantPermissionsToProcedure_TempTestProcedure'
		, @GrantList = @GrantList;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	EXECUTE AS LOGIN = N'GrantPermissionsToProcedure_TempTestLogin';

	EXEC GrantPermissionsToProcedureTests.GrantPermissionsToProcedure_TempTestProcedure;

	REVERT;
END;
GO

-- Run all tests in class.
EXEC tSQLt.Run @TestName = N'GrantPermissionsToProcedureTests';

GO

-- Delete the test class.
EXEC tSQLt.DropClass @ClassName = N'GrantPermissionsToProcedureTests';
