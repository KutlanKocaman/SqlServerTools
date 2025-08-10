/*

This script contains tests for SyncServerPermissionsAcrossAG,
defined here:
https://github.com/KutlanKocaman/SqlServerTools/blob/main/Procedures/SyncServerPermissionsAcrossAG.sql

Before running this script, make sure to:
1. Run the script above to create the SyncServerPermissionsAcrossAG procedure.
2. Install the tSQLt framework in the UtilityDb database.
3. Replace all 'MyAvailabilityGroup' with your availability group name.
4. Ensure you have at least 2 servers in the availability group.

These tests create logins and grants those logins permissions across the availability group.
All objects created are deleted afterwards.

Do not run this script on a Production database.

*/

USE UtilityDb;
GO

-- Create the test class for SyncServerPermissionsAcrossAG tests
EXEC tSQLt.NewTestClass @ClassName = N'SyncServerPermissionsAcrossAGTests';

GO

-- Helper procedure to execute SQL on a linked server
CREATE PROCEDURE SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
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
		DECLARE @ServerComment NVARCHAR(MAX) = CHAR(13) + CHAR(10) +
			N'-- On server: ' + @ServerName + CHAR(13) + CHAR(10);
		
		IF EXISTS
		(
			SELECT 1
			FROM sys.servers
			WHERE name = N'SyncServerPermissionsAcrossAGTestsServer'
		)
		BEGIN;
			EXEC sys.sp_dropserver @server = N'SyncServerPermissionsAcrossAGTestsServer';
		END;
		
		SET @LinkedServerProvider =
			CASE
				WHEN CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) >= 16 -- SQL 2022+
					THEN N'MSOLEDBSQL'
				ELSE N'SQLNCLI11'
			END

		EXEC sys.sp_addlinkedserver
			@server = N'SyncServerPermissionsAcrossAGTestsServer'
			, @srvproduct = N''
			, @provider = @LinkedServerProvider
			, @datasrc = @ServerName;

		EXEC sys.sp_serveroption
			@server = N'SyncServerPermissionsAcrossAGTestsServer'
			, @optname = 'RPC out'
			, @optvalue = N'true';

		SET @Sql = @ServerComment + @Sql;

		IF @Sql IS NULL
			THROW 50000, 'Dynamic @Sql is NULL', 0;

		EXEC SyncServerPermissionsAcrossAGTestsServer.master.sys.sp_executesql
			@stmt = @Sql
			, @params = N'@OutputParam SQL_VARIANT OUTPUT'
			, @OutputParam = @OutputParam OUTPUT;

		EXEC sys.sp_dropserver @server = N'SyncServerPermissionsAcrossAGTestsServer';
	END TRY
	BEGIN CATCH;
		IF EXISTS
		(
			SELECT 1
			FROM sys.servers
			WHERE name = N'SyncServerPermissionsAcrossAGTestsServer'
		)
		BEGIN;
			EXEC sys.sp_dropserver @server = N'SyncServerPermissionsAcrossAGTestsServer';
		END;

		THROW;
	END CATCH;
END;
GO

-- Helper procedure to sync logins across AG (without permissions)
CREATE PROCEDURE SyncServerPermissionsAcrossAGTests.SyncLoginAcrossAG
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
	DECLARE @OutputParam SQL_VARIANT;

	DECLARE SyncCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_replicas ar
	INNER JOIN sys.availability_groups ag
		ON ar.group_id = ag.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN SyncCursor;

	FETCH NEXT FROM SyncCursor
	INTO @ServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		-- Check if login exists on target
		SET @Sql = N'
		USE master;
		SELECT @OutputParam = COUNT(*)
		FROM sys.server_principals
		WHERE name = N' + QUOTENAME(@LoginName, '''') + N';';

		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @ServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;

		-- Create login if it doesn't exist
		IF @OutputParam = 0
		BEGIN;
			SET @Sql =
				N'USE master;' + @CrLf +
				N'CREATE LOGIN ' + QUOTENAME(@LoginName) + @CrLf +
				N'WITH PASSWORD = ''TestPassword123!'';';

			EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
				@ServerName = @ServerName
				, @Sql = @Sql;
		END;
		
		FETCH NEXT FROM SyncCursor
		INTO @ServerName;
	END;

	CLOSE SyncCursor;
	DEALLOCATE SyncCursor;
END;
GO

-- Helper procedure to drop logins across AG
CREATE PROCEDURE SyncServerPermissionsAcrossAGTests.DropLoginAcrossAG
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
			N'IF EXISTS' + @CrLf +
			N'(' + @CrLf +
			N'	SELECT 1' + @CrLf +
			N'	FROM sys.server_principals' + @CrLf +
			N'	WHERE name = N' + QUOTENAME(@LoginName, '''') + @CrLf +
			N')' + @CrLf +
			N'BEGIN;' + @CrLf +
			N'	DROP LOGIN ' + QUOTENAME(@LoginName) + N';' + @CrLf +
			N'END;';
			
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
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
CREATE PROCEDURE SyncServerPermissionsAcrossAGTests.SetUp
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
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create login on the current server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncServerPermissionsAcrossAG_TempTestLogin
		WITH PASSWORD = ''TestPassword123!'';';
	
	-- Sync the login to the other replicas
	EXEC SyncServerPermissionsAcrossAGTests.SyncLoginAcrossAG
		@AvailabilityGroupName = 'MyAvailabilityGroup'
		, @LoginName = 'SyncServerPermissionsAcrossAG_TempTestLogin';
END;
GO

-- Common clean-up for all tests
CREATE PROCEDURE SyncServerPermissionsAcrossAGTests.CleanUp
AS
BEGIN;
	EXEC SyncServerPermissionsAcrossAGTests.DropLoginAcrossAG
		@AvailabilityGroupName = N'MyAvailabilityGroup'
		, @LoginName = N'SyncServerPermissionsAcrossAG_TempTestLogin';
		
	EXEC SyncServerPermissionsAcrossAGTests.DropLoginAcrossAG
		@AvailabilityGroupName = N'MyAvailabilityGroup'
		, @LoginName = N'SyncServerPermissionsAcrossAG_TempLoginToImpersonate';
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test DENY AVAILABILITY GROUP permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- DENY CONTROL ON AVAILABILITY GROUP on this server
	SET @Sql = N'
		USE master;
		DENY CONTROL ON AVAILABILITY GROUP::' + QUOTENAME(@AvailabilityGroupName) +
	      ' TO SyncServerPermissionsAcrossAG_TempTestLogin;'

	EXEC sys.sp_executesql @stmt = @Sql;
	
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- CONTROL AVAILABILITY GROUP should be denied on all replicas in the AG
	SET @Sql = N'
		USE master;

		DECLARE @ReplicaMetaDataId INT =
		(
			SELECT TOP (1) ar.replica_metadata_id
			FROM sys.availability_groups ag
			INNER JOIN sys.availability_replicas ar
				ON ag.group_id = ar.group_id
			WHERE ag.name = N''MyAvailabilityGroup''
				AND ar.replica_metadata_id IS NOT NULL
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''AVAILABILITY GROUP''
		  AND p.major_id = @ReplicaMetaDataId
		  AND p.permission_name = ''CONTROL''
		  AND p.state_desc = ''DENY'';';
		  
	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'DENY CONTROL ON AVAILABILITY GROUP should exist on ' + @TargetServerName;
		
		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test DENY AVAILABILITY GROUP permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- DENY CONTROL ON AVAILABILITY GROUP on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		DENY CONTROL ON AVAILABILITY GROUP::' + QUOTENAME(@AvailabilityGroupName) +
		N' TO SyncServerPermissionsAcrossAG_TempTestLogin;';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- DENY CONTROL ON AVAILABILITY GROUP should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		DECLARE @ReplicaMetaDataId INT =
		(
			SELECT TOP (1) ar.replica_metadata_id
			FROM sys.availability_groups ag
			INNER JOIN sys.availability_replicas ar
				ON ag.group_id = ar.group_id
			WHERE ag.name = N''MyAvailabilityGroup''
				AND ar.replica_metadata_id IS NOT NULL
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''AVAILABILITY GROUP''
		  AND p.major_id = @ReplicaMetaDataId
		  AND p.permission_name = ''CONTROL''
		  AND p.state_desc = ''DENY'';';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
		
	SET @Message = N'DENY CONTROL ON AVAILABILITY GROUP should be revoked on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test DENY ENDPOINT permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @EndpointName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Ensure requirements are met
	------------------------------------------------------------

	SELECT TOP (1) @EndpointName = name
	FROM sys.endpoints;

	IF @EndpointName IS NULL
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'This test requires an endpoint and none was found';
		
		RETURN;
	END;
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- DENY CONNECT ON ENDPOINT on source
	SET @Sql = N'
		USE master;

		DENY CONNECT ON ENDPOINT::' + QUOTENAME(@EndpointName) + '
		TO SyncServerPermissionsAcrossAG_TempTestLogin;';

	EXEC sys.sp_executesql @stmt = @Sql;
	
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- CONNECT ON ENDPOINT should be denied on all replicas in the AG
	SET @Sql = N'
		USE master;
	
		DECLARE @EndpointId INT =
		(
			SELECT endpoint_id
			FROM sys.endpoints
			WHERE name = N' + QUOTENAME(@EndpointName, '''') + N'
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''ENDPOINT''
		  AND p.major_id = @EndpointId
		  AND p.permission_name = ''CONNECT''
		  AND p.state_desc = ''DENY'';';
	  
	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'DENY CONNECT ON ENDPOINT should exist on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test DENY ENDPOINT permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @EndpointName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Ensure requirements are met
	------------------------------------------------------------

	SELECT TOP (1) @EndpointName = name
	FROM sys.endpoints;

	IF @EndpointName IS NULL
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'This test requires an endpoint and none was found';
		
		RETURN;
	END;
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Deny CONNECT ON ENDPOINT on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		DENY CONNECT ON ENDPOINT::' + QUOTENAME(@EndpointName) + N'
		TO SyncServerPermissionsAcrossAG_TempTestLogin;'

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- DENY CONNECT ON ENDPOINT should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;
		
		DECLARE @EndpointId INT =
		(
			SELECT endpoint_id
			FROM sys.endpoints
			WHERE name = N' + QUOTENAME(@EndpointName, '''') + N'
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''ENDPOINT''
		  AND p.major_id = @EndpointId
		  AND p.permission_name = ''CONNECT''
		  AND p.state_desc = ''DENY'';';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	SET @Message = N'DENY CONNECT ON ENDPOINT should be revoked on ' + @TargetServerName;
	
	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test DENY SERVER permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- DENY VIEW SERVER STATE on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;
		
		DENY VIEW SERVER STATE TO SyncServerPermissionsAcrossAG_TempTestLogin;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- VIEW SERVER STATE should be DENIED on all replicas in the AG
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.permission_name = ''VIEW SERVER STATE''
		  AND p.state_desc = ''DENY'';';
	
	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'DENY VIEW SERVER STATE should exist on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test DENY SERVER permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX)
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- DENY VIEW SERVER STATE on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = N'
			USE master;
			
			DENY VIEW SERVER STATE TO SyncServerPermissionsAcrossAG_TempTestLogin;';
			
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- DENY SERVER STATE should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.permission_name = ''VIEW SERVER STATE''
		  AND p.state_desc = ''DENY'';';

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;

	SET @Message = N'DENY VIEW SERVER STATE should be removed on ' + @TargetServerName;
	
	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test DENY SERVER_PRINCIPAL permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Create a login to impersonate on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncServerPermissionsAcrossAG_TempLoginToImpersonate
		WITH PASSWORD = ''TestPassword123!'';';

	-- Sync the login to impersonate to other servers in the AG
	EXEC SyncServerPermissionsAcrossAGTests.SyncLoginAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginName = N'SyncServerPermissionsAcrossAG_TempLoginToImpersonate';

	-- DENY IMPERSONATE on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		DENY IMPERSONATE ON LOGIN::SyncServerPermissionsAcrossAG_TempLoginToImpersonate
		TO SyncServerPermissionsAcrossAG_TempTestLogin;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- IMPERSONATE should be denied on all replicas in the AG
	SET @Sql = N'
		USE master;

		DECLARE @LoginId INT =
		(
			SELECT principal_id
			FROM sys.server_principals
			WHERE name = N''SyncServerPermissionsAcrossAG_TempLoginToImpersonate''
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''SERVER_PRINCIPAL''
		  AND p.major_id = @LoginId
		  AND p.permission_name = ''IMPERSONATE''
		  AND p.state_desc = ''DENY'';';
	  
	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'DENY IMPERSONATE should exist on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test DENY SERVER_PRINCIPAL permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Create a login to impersonate on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncServerPermissionsAcrossAG_TempLoginToImpersonate
		WITH PASSWORD = ''TestPassword123!'';';

	-- Sync the login to impersonate to other servers in the AG
	EXEC SyncServerPermissionsAcrossAGTests.SyncLoginAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginName = N'SyncServerPermissionsAcrossAG_TempLoginToImpersonate';

	-- Deny IMPERSONATE on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = N'
			USE master;
			
			DENY IMPERSONATE ON LOGIN::SyncServerPermissionsAcrossAG_TempLoginToImpersonate
			TO SyncServerPermissionsAcrossAG_TempTestLogin;';
			
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- DENY IMPERSONATE should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		DECLARE @LoginId INT =
		(
			SELECT principal_id
			FROM sys.server_principals
			WHERE name = N''SyncServerPermissionsAcrossAG_TempLoginToImpersonate''
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''SERVER_PRINCIPAL''
		  AND p.major_id = @LoginId
		  AND p.permission_name = ''IMPERSONATE''
		  AND p.state_desc = ''DENY'';';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
		
	SET @Message = N'DENY IMPERSONATE should be revoked on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT AVAILABILITY GROUP permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Grant CONTROL ON AVAILABILITY GROUP on this server
	SET @Sql = N'
		USE master;

		GRANT CONTROL ON AVAILABILITY GROUP::' + QUOTENAME(@AvailabilityGroupName) +
	      ' TO SyncServerPermissionsAcrossAG_TempTestLogin;'

	EXEC sys.sp_executesql
		@stmt = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- CONTROL ON AVAILABILITY GROUP should be granted on all replicas in the AG
	SET @Sql = N'
		USE master;

		DECLARE @ReplicaMetaDataId INT =
		(
			SELECT TOP (1) ar.replica_metadata_id
			FROM sys.availability_groups ag
			INNER JOIN sys.availability_replicas ar
				ON ag.group_id = ar.group_id
			WHERE ag.name = N''MyAvailabilityGroup''
				AND ar.replica_metadata_id IS NOT NULL
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''AVAILABILITY GROUP''
		  AND p.major_id = @ReplicaMetaDataId
		  AND p.permission_name = ''CONTROL''
		  AND p.state_desc = ''GRANT'';';

	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'CONTROL ON AVAILAIBILITY GROUP should be granted on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT AVAILABILITY GROUP permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Grant CONTROL ON AVAILABILITY GROUP on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		GRANT CONTROL ON AVAILABILITY GROUP::' + QUOTENAME(@AvailabilityGroupName) +
			' TO SyncServerPermissionsAcrossAG_TempTestLogin;';

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		 @ServerName = @TargetServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- CONTROL ON AVAILABILITY GROUP should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		DECLARE @ReplicaMetaDataId INT =
		(
			SELECT TOP (1) ar.replica_metadata_id
			FROM sys.availability_groups ag
			INNER JOIN sys.availability_replicas ar
				ON ag.group_id = ar.group_id
			WHERE ag.name = N''MyAvailabilityGroup''
				AND ar.replica_metadata_id IS NOT NULL
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''AVAILABILITY GROUP''
		  AND p.major_id = @ReplicaMetaDataId
		  AND p.permission_name = ''CONTROL''
		  AND p.state_desc = ''GRANT'';';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
		
	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = N'AG permission should be revoked on target';
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT ENDPOINT permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @EndpointName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Ensure requirements are met
	------------------------------------------------------------

	SELECT TOP (1) @EndpointName = name
	FROM sys.endpoints;

	IF @EndpointName IS NULL
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'This test requires an endpoint and none was found';
		
		RETURN;
	END;
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Grant CONNECT ON ENDPOINT on this server
	SET @Sql = N'
		USE master;

		GRANT CONNECT ON ENDPOINT::' + QUOTENAME(@EndpointName) + '
		TO SyncServerPermissionsAcrossAG_TempTestLogin;';

	EXEC sys.sp_executesql
		@stmt = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- CONNECT ON ENDPOINT should be granted on all replicas in the AG
	SET @Sql = N'
		USE master;
	
		DECLARE @EndpointId INT =
		(
			SELECT endpoint_id
			FROM sys.endpoints
			WHERE name = N' + QUOTENAME(@EndpointName, '''') + N'
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''ENDPOINT''
		  AND p.major_id = @EndpointId
		  AND p.permission_name = ''CONNECT''
		  AND p.state_desc = ''GRANT'';';

	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'CONNECT ON ENDPOINT permission should be granted on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT ENDPOINT permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @EndpointName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Ensure requirements are met
	------------------------------------------------------------

	SELECT TOP (1) @EndpointName = name
	FROM sys.endpoints;

	IF @EndpointName IS NULL
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'This test requires an endpoint and none was found';
		
		RETURN;
	END;
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Grant CONNECT ON ENDPOINT on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		GRANT CONNECT ON ENDPOINT::' + QUOTENAME(@EndpointName) + N'
		TO SyncServerPermissionsAcrossAG_TempTestLogin;';
		
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- CONNECT ON ENDPOINT should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		DECLARE @EndpointId INT =
		(
			SELECT endpoint_id
			FROM sys.endpoints
			WHERE name = N' + QUOTENAME(@EndpointName, '''') + N'
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''ENDPOINT''
		  AND p.major_id = @EndpointId
		  AND p.permission_name = ''CONNECT''
		  AND p.state_desc = ''GRANT'';';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	SET @Message = N'CONNECT ON ENDPOINT permission should be revoked on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT SERVER permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Grant VIEW SERVER STATE on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		GRANT VIEW SERVER STATE TO SyncServerPermissionsAcrossAG_TempTestLogin;';
	
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- VIEW SERVER STATE should be granted on all replicas in the AG
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.permission_name = ''VIEW SERVER STATE''
		  AND p.state_desc = ''GRANT'';';
	  
	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;

		SET @Message = N'VIEW SERVER STATE should be granted on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT SERVER permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Grant VIEW SERVER STATE on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = N'
			USE master;

			GRANT VIEW SERVER STATE TO SyncServerPermissionsAcrossAG_TempTestLogin;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- VIEW SERVER STATE should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.permission_name = ''VIEW SERVER STATE''
		  AND p.state_desc = ''GRANT'';';

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			@ServerName = @TargetServerName
			, @Sql = @Sql
			, @OutputParam = @OutputParam OUTPUT;

	SET @Message = N'VIEW SERVER STATE permission should be revoked on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT SERVER_PRINCIPAL permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);

	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Create a login to impersonate on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncServerPermissionsAcrossAG_TempLoginToImpersonate
		WITH PASSWORD = ''TestPassword123!'';';

	-- Sync the login to impersonate to other servers in the AG
	EXEC SyncServerPermissionsAcrossAGTests.SyncLoginAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginName = N'SyncServerPermissionsAcrossAG_TempLoginToImpersonate';

	-- Grant IMPERSONATE on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		GRANT IMPERSONATE ON LOGIN::SyncServerPermissionsAcrossAG_TempLoginToImpersonate
		TO SyncServerPermissionsAcrossAG_TempTestLogin;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- IMPERSONATE should be granted on all replicas in the AG
	SET @Sql = N'
		USE master;

		DECLARE @loginId INT =
		(
			SELECT principal_id
			FROM sys.server_principals
			WHERE name = N''SyncServerPermissionsAcrossAG_TempLoginToImpersonate''
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''SERVER_PRINCIPAL''
		  AND p.major_id = @loginId
		  AND p.permission_name = ''IMPERSONATE''
		  AND p.state_desc = ''GRANT'';';

	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'IMPERSONATE permission should be granted on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT SERVER_PRINCIPAL permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Create a login to impersonate on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncServerPermissionsAcrossAG_TempLoginToImpersonate
		WITH PASSWORD = ''TestPassword123!'';';

	-- Sync the login to impersonate to other servers in the AG
	EXEC SyncServerPermissionsAcrossAGTests.SyncLoginAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginName = N'SyncServerPermissionsAcrossAG_TempLoginToImpersonate';

	--Grant IMPERSONATE on another server in the AG
	SELECT TOP(1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = N'
			USE master;
			
			GRANT IMPERSONATE ON LOGIN::SyncServerPermissionsAcrossAG_TempLoginToImpersonate
			TO SyncServerPermissionsAcrossAG_TempTestLogin;';
	
	-----------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- IMPERSONATE should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		DECLARE @LoginId INT =
		(
			SELECT principal_id
			FROM sys.server_principals
			WHERE name = N''SyncServerPermissionsAcrossAG_TempLoginToImpersonate''
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''SERVER_PRINCIPAL''
		  AND p.major_id = @LoginId
		  AND p.permission_name = ''IMPERSONATE''
		  AND p.state_desc = ''GRANT'';';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
		
	SET @Message = N'IMPERSONATE permission should be revoked on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT_WITH_GRANT_OPTION AVAILABILITY GROUP permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Grant CONTROL ON AVAILABILITY GROUP WITH GRANT OPTION on this server
	SET @Sql = N'
		USE master;
		GRANT CONTROL ON AVAILABILITY GROUP::' + QUOTENAME(@AvailabilityGroupName) +
	      ' TO SyncServerPermissionsAcrossAG_TempTestLogin WITH GRANT OPTION;';

	EXEC sys.sp_executesql @stmt = @Sql;
	
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- CONTROL ON AVAILABILITY GROUP WITH GRANT OPTION should be granted on all replicas in the AG
	SET @Sql = N'
		USE master;

		DECLARE @ReplicaMetaDataId INT =
		(
			SELECT TOP (1) ar.replica_metadata_id
			FROM sys.availability_groups ag
			INNER JOIN sys.availability_replicas ar
				ON ag.group_id = ar.group_id
			WHERE ag.name = N''MyAvailabilityGroup''
				AND ar.replica_metadata_id IS NOT NULL
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''AVAILABILITY GROUP''
		  AND p.major_id = @ReplicaMetaDataId
		  AND p.permission_name = ''CONTROL''
		  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';
	
	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'AG permission should be granted with GRANT OPTION on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT_WITH_GRANT_OPTION AVAILABILITY GROUP permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Grant CONTROL ON AVAILABILITY GROUP WITH GRANT OPTION on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;
		GRANT CONTROL ON AVAILABILITY GROUP::' + QUOTENAME(@AvailabilityGroupName) +
		 ' TO SyncServerPermissionsAcrossAG_TempTestLogin WITH GRANT OPTION;';

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- CONTROL ON AVAILABILITY GROUP WITH GRANT OPTION should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		DECLARE @ReplicaMetaDataId INT =
		(
			SELECT TOP (1) ar.replica_metadata_id
			FROM sys.availability_groups ag
			INNER JOIN sys.availability_replicas ar
				ON ag.group_id = ar.group_id
			WHERE ag.name = N''MyAvailabilityGroup''
				AND ar.replica_metadata_id IS NOT NULL
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''AVAILABILITY GROUP''
		  AND p.major_id = @ReplicaMetaDataId
		  AND p.permission_name = ''CONTROL''
		  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
		
	SET @Message =
		N'GRANT_WITH_GRANT_OPTION AG permission should be revoked on '+ @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT_WITH_GRANT_OPTION ENDPOINT permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @EndpointName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Ensure requirements are met
	------------------------------------------------------------

	SELECT TOP (1) @EndpointName = name
	FROM sys.endpoints;

	IF @EndpointName IS NULL
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'This test requires an endpoint and none was found';
		
		RETURN;
	END;
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Grant CONNECT ON ENDPOINT WITH GRANT OPTION on this server
	SET @Sql = N'
		USE master;

		GRANT CONNECT ON ENDPOINT::' + QUOTENAME(@EndpointName) + '
		TO SyncServerPermissionsAcrossAG_TempTestLogin WITH GRANT OPTION;';

	EXEC sys.sp_executesql @stmt = @Sql;
	
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- CONNECT ON ENDPOINT WITH GRANT OPTION should be granted on all replicas in the AG
	SET @Sql = N'
		USE master;

		DECLARE @EndpointId INT =
		(
			SELECT endpoint_id
			FROM sys.endpoints
			WHERE name = N' + QUOTENAME(@EndpointName, '''') + N'
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''ENDPOINT''
		  AND p.major_id = @EndpointId
		  AND p.permission_name = ''CONNECT''
		  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';

	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message =
			N'CONNECT ON ENDPOINT WITH GRANT OPTION should be granted on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT_WITH_GRANT_OPTION ENDPOINT permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @EndpointName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Ensure requirements are met
	------------------------------------------------------------

	SELECT TOP (1) @EndpointName = name
	FROM sys.endpoints;

	IF @EndpointName IS NULL
	BEGIN;
		EXEC tSQLt.Fail
			@Message0 = N'This test requires an endpoint and none was found';
		
		RETURN;
	END;
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Grant CONNECT ON ENDPOINT WITH GRANT OPTION on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	SET @Sql = N'
		USE master;

		GRANT CONNECT ON ENDPOINT::' + QUOTENAME(@EndpointName) + N'
		TO SyncServerPermissionsAcrossAG_TempTestLogin WITH GRANT OPTION;'

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		 @ServerName = @TargetServerName
		, @Sql = @Sql;
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- CONNECT ON ENDPOINT WITH GRANT OPTION should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		DECLARE @EndpointId INT =
		(
			SELECT endpoint_id
			FROM sys.endpoints 
			WHERE name = N' + QUOTENAME(@EndpointName, '''') + N'
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''ENDPOINT''
		  AND p.major_id = @EndpointId
		  AND p.permission_name = ''CONNECT''
		  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
	
	SET @Message =
		N'CONNECT ON ENDPOINT WITH GRANT OPTION should be revoked on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT_WITH_GRANT_OPTION SERVER is downgraded to GRANT]
	-- test for two step logic - remove GRANT_WITH_GRANT_OPTION and add GRANT
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- GRANT VIEW SERVER STATE on this server
	EXEC sys.sp_executesql
		@stmt = N'
			USE master;
			
			GRANT VIEW SERVER STATE TO SyncServerPermissionsAcrossAG_TempTestLogin;';

	-- GRANT VIEW SERVER STATE WITH GRANT OPTION on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = N'
			USE master;
			
			GRANT VIEW SERVER STATE TO SyncServerPermissionsAcrossAG_TempTestLogin
			WITH GRANT OPTION;';
			
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- VIEW SERVER STATE WITH GRANT OPTION shouldn't exist on @TargetServerName
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.permission_name = ''VIEW SERVER STATE''
		  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;

	SET @Message = N'GRANT_WITH_GRANT_OPTION should be removed on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
	
	-- VIEW SERVER STATE without GRANT OPTION should exist on @TargetServerName
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.permission_name = ''VIEW SERVER STATE''
		  AND p.state_desc = ''GRANT'';';

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
		
	SET @Message = N'GRANT VIEW SERVER STATE should exist on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 1
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT_WITH_GRANT_OPTION SERVER permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- GRANT VIEW SERVER STATE WITH GRANT OPTION on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		GRANT VIEW SERVER STATE
		TO SyncServerPermissionsAcrossAG_TempTestLogin
		WITH GRANT OPTION;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- GRANT VIEW SERVER STATE WITH GRANT OPTION should be granted on all replicas in the AG
	SET @Sql = N'
		USE master;

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.permission_name = ''VIEW SERVER STATE''
		  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';
	  
	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor 
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'GRANT_WITH_GRANT_OPTION permission should exist on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT_WITH_GRANT_OPTION SERVER permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Grant VIEW SERVER STATE WITH GRANT OPTION on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = N'
			USE master;
			
			GRANT VIEW SERVER STATE TO SyncServerPermissionsAcrossAG_TempTestLogin
			WITH GRANT OPTION;';
			
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- VIEW SERVER STATE WITH GRANT OPTION should be revoked on @TargetServerName
	SET @Sql = N'
	USE master;

	SELECT @OutputParam = COUNT(*)
	FROM sys.server_permissions p
	INNER JOIN sys.server_principals s
		ON p.grantee_principal_id = s.principal_id
	WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
	  AND p.permission_name = ''VIEW SERVER STATE''
	  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';

	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;
		
	SET @Message = N'VIEW SERVER STATE WITH GRANT OPTION should be revoked on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT_WITH_GRANT_OPTION SERVER_PRINCIPAL permissions are added]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Create a login to impersonate on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncServerPermissionsAcrossAG_TempLoginToImpersonate
		WITH PASSWORD = ''TestPassword123!'';';

	-- Sync the login to impersonate to other servers in the AG
	EXEC SyncServerPermissionsAcrossAGTests.SyncLoginAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginName = N'SyncServerPermissionsAcrossAG_TempLoginToImpersonate';

	-- Grant permission on source WITH GRANT OPTION
	EXEC sys.sp_executesql @stmt = N'
		USE master;
		
		GRANT IMPERSONATE ON LOGIN::SyncServerPermissionsAcrossAG_TempLoginToImpersonate
		TO SyncServerPermissionsAcrossAG_TempTestLogin WITH GRANT OPTION;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- IMPERSONATE WITH GRANT OPTION should be granted on all replicas in the AG
	SET @Sql = N'
		USE master;

		DECLARE @LoginId INT =
		(
			SELECT principal_id
			FROM sys.server_principals
			WHERE name = N''SyncServerPermissionsAcrossAG_TempLoginToImpersonate''
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''SERVER_PRINCIPAL''
		  AND p.major_id = @LoginId
		  AND p.permission_name = ''IMPERSONATE''
		  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';
	  
	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'IMPERSONATE WITH GRANT OPTION should be granted on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;
		
		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
	
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test GRANT_WITH_GRANT_OPTION SERVER_PRINCIPAL permissions are revoked]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------
	
	-- Create a login to impersonate on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		CREATE LOGIN SyncServerPermissionsAcrossAG_TempLoginToImpersonate
		WITH PASSWORD = ''TestPassword123!'';';

	-- Sync the login to impersonate to other servers in the AG
	EXEC SyncServerPermissionsAcrossAGTests.SyncLoginAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginName = N'SyncServerPermissionsAcrossAG_TempLoginToImpersonate';

	-- Grant IMPERSONATE on another server in the AG
	SELECT TOP (1) @TargetServerName = ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	-- Introduce permission only on target
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = N'
			USE master;
			
			GRANT IMPERSONATE ON LOGIN::SyncServerPermissionsAcrossAG_TempLoginToImpersonate
			TO SyncServerPermissionsAcrossAG_TempTestLogin WITH GRANT OPTION;';
			
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------
	
	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
	
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------

	-- IMPERSONATE WITH GRANT OPTION should be revoked on @TargetServerName
	SET @Sql = N'
		USE master;

		DECLARE @LoginId INT =
		(
			SELECT principal_id
			FROM sys.server_principals
			WHERE name = N''SyncServerPermissionsAcrossAG_TempLoginToImpersonate''
		);

		SELECT @OutputParam = COUNT(*)
		FROM sys.server_permissions p
		INNER JOIN sys.server_principals s
			ON p.grantee_principal_id = s.principal_id
		WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
		  AND p.class_desc = ''SERVER_PRINCIPAL''
		  AND p.major_id = @LoginId
		  AND p.permission_name = ''IMPERSONATE''
		  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';
	
	EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
		@ServerName = @TargetServerName
		, @Sql = @Sql
		, @OutputParam = @OutputParam OUTPUT;

	SET @Message = N'IMPERSONATE WITH GRANT OPTION should be revoked on ' + @TargetServerName;

	EXEC tSQLt.AssertEquals
		@Expected = 0
		, @Actual = @OutputParam
		, @Message = @Message;
END;
GO

--[@tSQLt:NoTransaction]('SyncServerPermissionsAcrossAGTests.CleanUp')
CREATE PROCEDURE
	SyncServerPermissionsAcrossAGTests
	.[test mixed permission states are handled correctly]
AS
BEGIN;
	DECLARE @AvailabilityGroupName SYSNAME = N'MyAvailabilityGroup';
	DECLARE @TargetServerName SYSNAME;
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @OutputParam SQL_VARIANT;
	DECLARE @Message NVARCHAR(MAX);
	
	------------------------------------------------------------
	-- Arrange
	------------------------------------------------------------

	-- Apply mixed permissions on this server
	EXEC sys.sp_executesql @stmt = N'
		USE master;

		GRANT VIEW SERVER STATE TO SyncServerPermissionsAcrossAG_TempTestLogin;
		DENY CONNECT SQL TO SyncServerPermissionsAcrossAG_TempTestLogin;
		GRANT ALTER ANY LOGIN TO SyncServerPermissionsAcrossAG_TempTestLogin WITH GRANT OPTION;';
		
	------------------------------------------------------------
	-- Act
	------------------------------------------------------------

	EXEC UtilityDb.dbo.SyncServerPermissionsAcrossAG
		@AvailabilityGroupName = @AvailabilityGroupName
		, @LoginNamePattern = 'SyncServerPermissionsAcrossAG_TempTestLogin';
		
	------------------------------------------------------------
	-- Assert
	------------------------------------------------------------
	
	-- Loop through the other servers in the availability group
	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_groups ag
	INNER JOIN sys.availability_replicas ar
		ON ag.group_id = ar.group_id
	WHERE ag.name = @AvailabilityGroupName
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @TargetServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		-- GRANT VIEW SERVER STATE should exist
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.server_permissions p
			INNER JOIN sys.server_principals s
				ON p.grantee_principal_id = s.principal_id
			WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
			  AND p.permission_name = ''VIEW SERVER STATE''
			  AND p.state_desc = ''GRANT'';';

		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;

		SET @Message = N'GRANT VIEW SERVER STATE should exist on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		-- DENY CONNECT SQL should exist
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.server_permissions p
			INNER JOIN sys.server_principals s
				ON p.grantee_principal_id = s.principal_id
			WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
			  AND p.permission_name = ''CONNECT SQL''
			  AND p.state_desc = ''DENY'';';

		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message = N'DENY CONNECT SQL should exist on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		-- Check GRANT ALTER ANY LOGIN WITH GRANT OPTION
		SET @Sql = N'
			USE master;

			SELECT @OutputParam = COUNT(*)
			FROM sys.server_permissions p
			INNER JOIN sys.server_principals s
				ON p.grantee_principal_id = s.principal_id
			WHERE s.name = N''SyncServerPermissionsAcrossAG_TempTestLogin''
			  AND p.permission_name = ''ALTER ANY LOGIN''
			  AND p.state_desc = ''GRANT_WITH_GRANT_OPTION'';';

		EXEC SyncServerPermissionsAcrossAGTests.ExecuteSqlOnLinkedServer
			 @ServerName = @TargetServerName
			 , @Sql = @Sql
			 , @OutputParam = @OutputParam OUTPUT;
			 
		SET @Message =
			N'GRANT ALTER ANY LOGIN WITH GRANT OPTION should exist on ' + @TargetServerName;

		EXEC tSQLt.AssertEquals
			@Expected = 1
			, @Actual = @OutputParam
			, @Message = @Message;

		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @TargetServerName;
	END;
		
	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END;
GO

-- Run all tests.
EXEC tSQLt.Run @TestName = N'SyncServerPermissionsAcrossAGTests';

GO

-- Delete the test class.
EXEC tSQLt.DropClass @ClassName = N'SyncServerPermissionsAcrossAGTests';
