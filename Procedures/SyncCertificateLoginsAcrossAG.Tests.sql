/*

This script contains tests for the procedure SyncCertificateLoginsAcrossAG,
defined here:
https://github.com/KutlanKocaman/SqlServerTools/blob/main/Procedures/SyncCertificateLoginsAcrossAG.sql

Before running this script, make sure to:
1. Run the script above to create the SyncCertificateLoginsAcrossAG procedure.
2. Install the tSQLt framework in the UtilityDb database.
3. Replace all 'MyAvailabilityGroup' with your availability group name.
4. Ensure you have at least 2 servers in the availability group.

These tests create certificates and logins across the availability group.
All objects created are deleted afterwards.

Do not run this script on a Production database.

*/

USE UtilityDb;
GO

-- Create the test class for SyncCertificateLoginsAcrossAG tests
EXEC tSQLt.NewTestClass @ClassName = N'SyncCertificateLoginsAcrossAGTests';

GO

-- Helper procedure to execute SQL on a linked server
CREATE PROCEDURE SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
(
	@ServerName SYSNAME
	, @Sql NVARCHAR(MAX)
	, @OutputParam SQL_VARIANT = NULL OUTPUT
)
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	BEGIN TRY;
		DECLARE @LinkedServerProvider NVARCHAR(128);
		DECLARE @ServerComment NVARCHAR(MAX) =
			CHAR(13) + CHAR(10) +
			N'-- On server: ' + @ServerName + CHAR(13) + CHAR(10);
		
		IF EXISTS
		(
			SELECT 1
			FROM sys.servers
			WHERE name = N'SyncCertificateLoginsAcrossAGTestsServer'
		)
		BEGIN;
			EXEC sys.sp_dropserver @server = N'SyncCertificateLoginsAcrossAGTestsServer';
		END;
		
		SET @LinkedServerProvider =
			CASE
				WHEN CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) >= 16 -- SQL 2022+
					THEN N'MSOLEDBSQL'
				ELSE N'SQLNCLI11'
			END

		EXEC sys.sp_addlinkedserver
			@server = N'SyncCertificateLoginsAcrossAGTestsServer'
			, @srvproduct = N''
			, @provider = @LinkedServerProvider
			, @datasrc = @ServerName;

		EXEC sys.sp_serveroption
			@server = N'SyncCertificateLoginsAcrossAGTestsServer'
			, @optname = 'RPC out'
			, @optvalue = N'true';

		SET @Sql = @ServerComment + @Sql;

		IF @Sql IS NULL
			THROW 50000, 'Dynamic @Sql is NULL', 0;

		EXEC SyncCertificateLoginsAcrossAGTestsServer.master.sys.sp_executesql
			@stmt = @Sql
			, @params = N'@OutputParam SQL_VARIANT OUTPUT'
			, @OutputParam = @OutputParam OUTPUT;

		EXEC sys.sp_dropserver @server = N'SyncCertificateLoginsAcrossAGTestsServer';
	END TRY
	BEGIN CATCH;
		IF EXISTS
		(
			SELECT 1
			FROM sys.servers
			WHERE name = N'SyncCertificateLoginsAcrossAGTestsServer'
		)
		BEGIN;
			EXEC sys.sp_dropserver @server = N'SyncCertificateLoginsAcrossAGTestsServer';
		END;

		THROW;
	END CATCH;
END;
GO

-- Helper procedure to drop certificate-mapped logins across AG
CREATE PROCEDURE SyncCertificateLoginsAcrossAGTests.DropCertificateLoginAcrossAG
(
	@AvailabilityGroupName SYSNAME
	, @LoginName SYSNAME
)
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @ServerName NVARCHAR(256);

	DECLARE CleanupCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_replicas ar
	INNER JOIN sys.availability_groups ag
		ON ar.group_id = ag.group_id
	WHERE ag.name = @AvailabilityGroupName
	ORDER BY ar.replica_server_name;

	OPEN CleanupCursor;

	FETCH NEXT FROM CleanupCursor
	INTO @ServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		SET @Sql =
			N'USE master;' + @CrLf +
			@CrLf +
			N'IF EXISTS' + @CrLf +
			N'(' + @CrLf +
			N'	SELECT 1' + @CrLf +
			N'	FROM sys.server_principals' + @CrLf +
			N'	WHERE name = N' + QUOTENAME(@LoginName, '''') + N'' + @CrLf +
			N'		AND type_desc = ''CERTIFICATE_MAPPED_LOGIN''' + @CrLf +
			N')' + @CrLf +
			N'BEGIN;' + @CrLf +
			N'	DROP LOGIN ' + QUOTENAME(@LoginName) + N';' + @CrLf +
			N'END;';
			
		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql;
		
		FETCH NEXT FROM CleanupCursor
		INTO @ServerName;
	END;

	CLOSE CleanupCursor;
	DEALLOCATE CleanupCursor;
END;
GO

-- Helper procedure to drop certificates across AG
CREATE PROCEDURE SyncCertificateLoginsAcrossAGTests.DropCertificateAcrossAG
(
	@AvailabilityGroupName SYSNAME
	, @CertificateName SYSNAME
)
AS
BEGIN;
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @ServerName NVARCHAR(256);

	DECLARE CleanupCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_replicas ar
	INNER JOIN sys.availability_groups ag
		ON ar.group_id = ag.group_id
	WHERE ag.name = @AvailabilityGroupName
	ORDER BY ar.replica_server_name;

	OPEN CleanupCursor;

	FETCH NEXT FROM CleanupCursor
	INTO @ServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		SET @Sql =
			N'USE master;' + @CrLf +
			@CrLf +
			N'IF EXISTS' + @CrLf +
			N'(' + @CrLf +
			N'	SELECT 1' + @CrLf +
			N'	FROM sys.certificates' + @CrLf +
			N'	WHERE name = N' + QUOTENAME(@CertificateName, '''') + N'' + @CrLf +
			N')' + @CrLf +
			N'BEGIN;' + @CrLf +
			N'	DROP CERTIFICATE ' + QUOTENAME(@CertificateName) + N';' + @CrLf +
			N'END;';
			
		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql;
		
		FETCH NEXT FROM CleanupCursor
		INTO @ServerName;
	END;

	CLOSE CleanupCursor;
	DEALLOCATE CleanupCursor;
END;
GO

-- Common setup for all tests
CREATE PROCEDURE SyncCertificateLoginsAcrossAGTests.SetUp
AS
BEGIN;
	------------------------------------------------------------
	-- Ensure requirements are met
	------------------------------------------------------------

	DECLARE @AvailabilityGroupId UNIQUEIDENTIFIER;
	
	SELECT @AvailabilityGroupId = group_id
	FROM sys.availability_groups
	WHERE name = N'MyAvailabilityGroup';

	IF @AvailabilityGroupId IS NULL
	OR NOT EXISTS
	(
		SELECT 1
		FROM sys.availability_replicas
		WHERE group_id = @AvailabilityGroupId
			AND replica_server_name <> @@SERVERNAME
	)
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'This test requires an availability group with at least two replicas';

		RETURN;
	END;
END;
GO

-- Common clean-up for all tests
CREATE PROCEDURE SyncCertificateLoginsAcrossAGTests.CleanUp
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	
	-- Clean up test objects
	EXEC SyncCertificateLoginsAcrossAGTests.DropCertificateLoginAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginName = N'SyncCertificateLoginsAcrossAG_TempTestLogin';

	EXEC SyncCertificateLoginsAcrossAGTests.DropCertificateAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @CertificateName = N'SyncCertificateLoginsAcrossAG_TempTestCert';
		
	EXEC SyncCertificateLoginsAcrossAGTests.DropCertificateLoginAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginName = N'SyncCertificateLoginsAcrossAG_TempTestLogin2';

	EXEC SyncCertificateLoginsAcrossAGTests.DropCertificateAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @CertificateName = N'SyncCertificateLoginsAcrossAG_TempTestCert2';
END;
GO

--[@tSQLt:NoTransaction]('SyncCertificateLoginsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncCertificateLoginsAcrossAGTests
	.[test cert and login are copied to other servers in AG]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	DECLARE @ServerName NVARCHAR(256);
	DECLARE @ExpectedPublicKey VARBINARY(MAX);

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create test certificate and login
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';

	SET @Sql = N'
		USE master;

		SELECT @ExpectedPublicKey = CERTENCODED(certificate_id)
		FROM master.sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC sys.sp_executesql
		@stmt = @Sql
		, @params = N'@ExpectedPublicKey VARBINARY(MAX) OUTPUT'
		, @ExpectedPublicKey = @ExpectedPublicKey OUTPUT;

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin
		FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncCertificateLoginsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncCertificateLoginsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	DECLARE AgServerCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AgServerCursor;

	FETCH NEXT FROM AgServerCursor
	INTO @ServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		-- Verify certificate exists on target server
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.certificates
			WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;
		
		SET @Message = N'Certificate should exist on ' + @ServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		-- Verify certificate has correct public key
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.certificates
			WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert''
				AND CERTENCODED(certificate_id) = '
					+ CONVERT(NVARCHAR(MAX), @ExpectedPublicKey, 1) + N';';
				
		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;
			
		SET @Message = N'Certificate should have correct public key on ' + @ServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		-- Verify login exists on target server
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.server_principals
			WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestLogin'';';

		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;
			
		SET @Message = N'Login should exist on ' + @ServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		FETCH NEXT FROM AgServerCursor
		INTO @ServerName;
	END;
	
	CLOSE AgServerCursor;
	DEALLOCATE AgServerCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncCertificateLoginsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncCertificateLoginsAcrossAGTests
	.[test cert and login are left unchanged when they already exist]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	DECLARE @ServerName NVARCHAR(256);
	DECLARE @PublicKey VARBINARY(MAX);
	DECLARE @ExpectedCertificateId INT;
	DECLARE @ExpectedPrincipalId INT;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create certificate and login on this server

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';

	SET @Sql = N'
		USE master;

		SELECT @PublicKey = CERTENCODED(certificate_id)
		FROM master.sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC sys.sp_executesql
		@stmt = @Sql
		, @params = N'@PublicKey VARBINARY(MAX) OUTPUT'
		, @PublicKey = @PublicKey OUTPUT;

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin
		FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert;';
		
	-- Create certificate and login on another server in the AG

	SELECT TOP (1) @ServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		FROM BINARY = ' + CONVERT(NVARCHAR(MAX), @PublicKey, 1) + N';';
		
	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql;
		
	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = N'
			USE master;

			CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin
			FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert;';
		
	-- Get the certificate_id to verify it doesn't change

	SET @Sql = N'
		USE master;

		SELECT @OutputParam = certificate_id
		FROM sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	SET @ExpectedCertificateId = CAST(@OutputParam AS INT);
	
	-- Get the principal_id to verify it doesn't change

	SET @Sql = N'
		USE master;

		SELECT @OutputParam = principal_id
		FROM sys.server_principals
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestLogin'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	SET @ExpectedPrincipalId = CAST(@OutputParam AS INT);
	
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncCertificateLoginsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncCertificateLoginsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- Certificate should still exist with same certificate_id (not recreated)

	SET @Sql = N'
		USE master;

		SELECT @OutputParam = certificate_id
		FROM sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	EXEC tSQLt.AssertEquals
		@Expected = @ExpectedCertificateId
		, @Actual = @OutputParam
		, @Message = N'Certificate should not be recreated when it already exists';
		
	-- Login should still exist with same principal_id (not recreated)

	SET @Sql = N'
		USE master;

		SELECT @OutputParam = principal_id
		FROM sys.server_principals
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestLogin'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	EXEC tSQLt.AssertEquals
		@Expected = @ExpectedPrincipalId
		, @Actual = @OutputParam
		, @Message = N'Login should not be recreated when it already exists';
END;
GO

/*
When @DeleteCertificateWithLogin = 1,
drop the certificate behind the login after dropping the login.

For example:
on this server: login1 mapped to cert1
on server2 in the AG: login1 mapped to cert2
run SyncCertificateLoginsAcrossAG:
	login1 is dropped on target because it has the wrong certificate behind it
	is cert2 dropped? if @DeleteCertificateWithLogin = 1 then yes
*/
--[@tSQLt:NoTransaction]('SyncCertificateLoginsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncCertificateLoginsAcrossAGTests
	.[test cert behind dropped-login is dropped when @DeleteCertificateWithLogin = 1]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @ServerName NVARCHAR(256);

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create certificate and login ONLY on target server
	SELECT TOP (1) @ServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql;
		
	SET @Sql = N'
		USE master;

		CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin
		FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert;';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncCertificateLoginsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncCertificateLoginsAcrossAG_TempTestLogin'
		, @DeleteCertificateWithLogin = 1;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- Certificate should have been dropped
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = N'Certificate should be dropped when @DeleteCertificateWithLogin = 1';
END;
GO

/*
When @DeleteCertificateWithLogin = 0,
don't drop certificate behind dropped login on target server

For example:
on this server: login1 mapped to cert1
on server2 in the AG: login1 mapped to cert2
run SyncCertificateLoginsAcrossAG:
	login1 is dropped on target because it has the wrong certificate behind it
	is cert2 dropped? if @DeleteCertificateWithLogin = 0 then no
*/
--[@tSQLt:NoTransaction]('SyncCertificateLoginsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncCertificateLoginsAcrossAGTests
	.[test cert behind dropped-login isn't dropped when @DeleteCertificateWithLogin = 0]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @ServerName NVARCHAR(256);

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create certificate and login ONLY on target server
	SELECT TOP (1) @ServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql;
		
	SET @Sql = N'
		USE master;

		CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin
		FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert;';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncCertificateLoginsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncCertificateLoginsAcrossAG_TempTestLogin'
		, @DeleteCertificateWithLogin = 0;
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- Certificate should NOT have been dropped
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;

	EXEC tSQLt.AssertEquals
		@Expected = 1
		, @Actual = @OutputParam
		, @Message = N'Certificate should not be dropped when @DeleteCertificateWithLogin = 0';
END;
GO

--[@tSQLt:NoTransaction]('SyncCertificateLoginsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncCertificateLoginsAcrossAGTests
	.[test cert on target server with correct public key is not recreated]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @ServerName NVARCHAR(256);
	DECLARE @PublicKey VARBINARY(MAX);
	DECLARE @ExpectedCertificateId INT;

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create certificate and login on source server.

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';
	
	SET @Sql = N'
		USE master;

		SELECT @PublicKey = CERTENCODED(certificate_id)
		FROM master.sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';
		
	EXEC sys.sp_executesql
		@stmt = @Sql
		, @params = N'@PublicKey VARBINARY(MAX) OUTPUT'
		, @PublicKey = @PublicKey OUTPUT;

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin
		FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert;';

	-- Create certificate with same public key on target server.

	SELECT TOP (1) @ServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		FROM BINARY = ' + CONVERT(NVARCHAR(MAX), @PublicKey, 1) + N';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql;
		
	-- Get the certificate_id to verify it doesn't change
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = certificate_id
		FROM sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	SET @ExpectedCertificateId = CAST(@OutputParam AS INT);
	
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncCertificateLoginsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncCertificateLoginsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- Certificate should still exist with same certificate_id (not recreated)
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = certificate_id
		FROM sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	EXEC tSQLt.AssertEquals
		@Expected = @ExpectedCertificateId
		, @Actual = @OutputParam
		, @Message = N'Certificate should not be recreated when public key matches';
END;
GO

--[@tSQLt:NoTransaction]('SyncCertificateLoginsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncCertificateLoginsAcrossAGTests
	.[test cert on target server with incorrect public key is re-created]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @ServerName NVARCHAR(256);
	DECLARE @ExpectedPublicKey VARBINARY(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create certificate and login on source server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';
	
	SET @Sql = N'
		USE master;

		SELECT @ExpectedPublicKey = CERTENCODED(certificate_id)
		FROM master.sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC sys.sp_executesql
		@stmt = @Sql
		, @params = N'@ExpectedPublicKey VARBINARY(MAX) OUTPUT'
		, @ExpectedPublicKey = @ExpectedPublicKey OUTPUT;

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin
		FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert;';

	-- Create certificate with same name but different public key on target server
	SELECT TOP (1) @ServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncCertificateLoginsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncCertificateLoginsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- Certificate should exist on target server
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	EXEC tSQLt.AssertEquals
		@Expected = 1
		, @Actual = @OutputParam
		, @Message = N'Certificate should exist on target server';

	-- Certificate should have correct public key (from source server)
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert''
			AND CERTENCODED(certificate_id) = '
				+ CONVERT(NVARCHAR(MAX), @ExpectedPublicKey, 1) + N';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;

	EXEC tSQLt.AssertEquals
		@Expected = 1
		, @Actual = @OutputParam
		, @Message = N'Certificate should have expected public key from source server';
END;
GO

--[@tSQLt:NoTransaction]('SyncCertificateLoginsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncCertificateLoginsAcrossAGTests
	.[test login on target server is dropped when it doesn't exist on this server]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @ServerName NVARCHAR(256);

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create certificate and login ONLY on target server (not on source)
	SELECT TOP (1) @ServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql;
		
	SET @Sql = N'
		USE master;

		CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin
		FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert;';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncCertificateLoginsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncCertificateLoginsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- The login should have been dropped from target server
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_principals
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestLogin'';';

	EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @ServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = N'Login should be dropped from target server when not on source';
END;
GO

--[@tSQLt:NoTransaction]('SyncCertificateLoginsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncCertificateLoginsAcrossAGTests
	.[test multiple logins matching @LoginNamePattern are synced]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	DECLARE @ServerName NVARCHAR(256);
	DECLARE @ExpectedPublicKey1 VARBINARY(MAX);
	DECLARE @ExpectedPublicKey2 VARBINARY(MAX);

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create first test certificate and login

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';

	SET @Sql = N'
		USE master;

		SELECT @ExpectedPublicKey1 = CERTENCODED(certificate_id)
		FROM master.sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

	EXEC sys.sp_executesql
		@stmt = @Sql
		, @params = N'@ExpectedPublicKey1 VARBINARY(MAX) OUTPUT'
		, @ExpectedPublicKey1 = @ExpectedPublicKey1 OUTPUT;

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin
		FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert;';
		
	-- Create second test certificate and login
	
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert2
		ENCRYPTION BY PASSWORD = N''TestPassword123!''
		WITH SUBJECT = N''Temporary certificate for testing SyncCertificateLoginsAcrossAG'';';

	SET @Sql = N'
		USE master;

		SELECT @ExpectedPublicKey2 = CERTENCODED(certificate_id)
		FROM master.sys.certificates
		WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert2'';';

	EXEC sys.sp_executesql
		@stmt = @Sql
		, @params = N'@ExpectedPublicKey2 VARBINARY(MAX) OUTPUT'
		, @ExpectedPublicKey2 = @ExpectedPublicKey2 OUTPUT;

	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncCertificateLoginsAcrossAG_TempTestLogin2
		FROM CERTIFICATE SyncCertificateLoginsAcrossAG_TempTestCert2;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncCertificateLoginsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncCertificateLoginsAcrossAG_TempTestLogin%';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	DECLARE AgServerCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AgServerCursor;

	FETCH NEXT FROM AgServerCursor
	INTO @ServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		-- Verify certificate 1 exists on target server
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.certificates
			WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert'';';

		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;
		
		SET @Message = N'Certificate 1 should exist on ' + @ServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		-- Verify certificate 1 has correct public key
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.certificates
			WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert''
				AND CERTENCODED(certificate_id) = '
					+ CONVERT(NVARCHAR(MAX), @ExpectedPublicKey1, 1) + N';';
				
		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;
			
		SET @Message = N'Certificate 1 should have correct public key on ' + @ServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		-- Verify login 1 exists on target server
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.server_principals
			WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestLogin'';';

		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;
			
		SET @Message = N'Login 1 should exist on ' + @ServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
			
		-- Verify certificate 2 exists on target server
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.certificates
			WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert2'';';

		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;
		
		SET @Message = N'Certificate 2 should exist on ' + @ServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		-- Verify certificate 2 has correct public key
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.certificates
			WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestCert2''
				AND CERTENCODED(certificate_id) = '
					+ CONVERT(NVARCHAR(MAX), @ExpectedPublicKey2, 1) + N';';
				
		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;
			
		SET @Message = N'Certificate 2 should have correct public key on ' + @ServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		-- Verify login 2 exists on target server
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.server_principals
			WHERE name = N''SyncCertificateLoginsAcrossAG_TempTestLogin2'';';

		EXEC SyncCertificateLoginsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;
			
		SET @Message = N'Login 2 should exist on target server on ' + @ServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		FETCH NEXT FROM AgServerCursor
		INTO @ServerName;
	END;
	
	CLOSE AgServerCursor;
	DEALLOCATE AgServerCursor;
END;
GO

-- Run all tests in class.
EXEC tSQLt.Run @TestName = N'SyncCertificateLoginsAcrossAGTests';

GO

-- Delete the test class.
EXEC tSQLt.DropClass @ClassName = N'SyncCertificateLoginsAcrossAGTests';
