/*

This procedure gets the server permissions for all logins whose name
matches the @LoginNamePattern specified and syncs them across
the availability group specified in @AvailabilityGroupName.
Permissions which exist on this server but not on another are added on the other.
Permissions which don't exist on this server but do on another are revoked on the other.

The default @LoginNamePattern is '%' meaning all logins will be synced.

The procedure just syncs permissions. The logins must already exist.

The procedure relies on the SQL Server Browser to connect to instances using ports other than 1433.

The procedure can't be used within a transaction because it uses sp_addlinkedserver.

Tests for this procedure are here:
https://github.com/KutlanKocaman/SqlServerTools/blob/main/Procedures/SyncServerPermissionsAcrossAG.Tests.sql

*/

IF DB_ID('UtilityDb') IS NULL
	CREATE DATABASE UtilityDb;

GO

USE UtilityDb;

IF OBJECT_ID('dbo.SyncServerPermissionsAcrossAG') IS NOT NULL
	DROP PROCEDURE dbo.SyncServerPermissionsAcrossAG;

GO

CREATE PROCEDURE dbo.SyncServerPermissionsAcrossAG
(
	@AvailabilityGroupName SYSNAME
	, @Debug BIT = 0 -- 1 = PRINT all dynamic SQL executed
	, @LoginNamePattern NVARCHAR(MAX) = N'%'
	, @LinkedServerProvider NVARCHAR(128) = NULL
)
AS
SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY;

	DECLARE @AvailabilityGroupId UNIQUEIDENTIFIER;
	DECLARE @IsContainedAvailabilityGroup BIT;
	DECLARE @Message NVARCHAR(MAX);
	DECLARE @Sql NVARCHAR(MAX);
	DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
	DECLARE @ServerName NVARCHAR(256);
	DECLARE @ServerComment NVARCHAR(MAX);
	DECLARE @SourceLoginName SYSNAME;
	DECLARE @PermissionName SYSNAME;
	DECLARE @ClassDesc NVARCHAR(60);
	DECLARE @ObjectIdentifier SYSNAME;

	-- Verify @AvailabilityGroupName.

	SELECT @AvailabilityGroupId = group_id
	FROM sys.availability_groups
	WHERE name = @AvailabilityGroupName;

	IF @AvailabilityGroupId IS NULL
	BEGIN;
		SET @Message = N'Availability group ''' +
			ISNULL(REPLACE(@AvailabilityGroupName, '''', ''''''), 'NULL') +
			''' not found.';

		THROW 50000, @Message, 0;
	END;

	-- Make sure it's not a contained availability group.

	IF CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) >= 16
	BEGIN;
		SET @Sql = @CrLf +
			N'-- on the current server.' + @CrLf +
			N'USE master;' + @CrLf +
			@CrLf +
			N'SELECT @IsContainedAvailabilityGroup = is_contained' + @CrLf +
			N'FROM sys.availability_groups' + @CrLf +
			N'WHERE group_id = @AvailabilityGroupId;' + @CrLf;
	
		IF @Debug = 1
			PRINT @Sql;
	
		IF @Sql IS NULL
			THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 0;

		EXEC sys.sp_executesql
			@stmt = @Sql
			, @params = N'@AvailabilityGroupId SYSNAME
				, @IsContainedAvailabilityGroup BIT OUTPUT'
			, @AvailabilityGroupId = @AvailabilityGroupId
			, @IsContainedAvailabilityGroup = @IsContainedAvailabilityGroup OUTPUT;

		IF @IsContainedAvailabilityGroup = 1
		BEGIN;
			SET @Message = N'Availability group ''' + @AvailabilityGroupName +
				''' is a contained availability group.';

			THROW 50000, @Message, 0;
		END;
	END;

	-- Get the logins for which to sync permissions across the availability group.

	CREATE TABLE #Logins_Source
	(
		LoginName SYSNAME
	);

	SET @Sql = @CrLf +
		N'-- on the current server.' + @CrLf +
		N'USE master;' + @CrLf +
		@CrLf +
		N'INSERT INTO #Logins_Source' + @CrLf +
		N'(' + @CrLf +
		N'	LoginName' + @CrLf +
		N')' + @CrLf +
		N'SELECT LoginName = sp.name' + @CrLf +
		N'FROM sys.server_principals sp' + @CrLf +
		N'WHERE sp.name LIKE @LoginNamePattern;' + @CrLf;
	
	IF @Debug = 1
		PRINT @Sql;
	
	IF @Sql IS NULL
		THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 1;

	EXEC sys.sp_executesql
		@stmt = @Sql
		, @params = N'@LoginNamePattern NVARCHAR(MAX)'
		, @LoginNamePattern = @LoginNamePattern;

	-- Figure out which provider to use to set up the linked server.
	
	IF @LinkedServerProvider IS NULL
	BEGIN;
		SET @LinkedServerProvider =
			CASE
				WHEN CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) >= 16 -- SQL 2022+
					THEN N'MSOLEDBSQL'
				ELSE N'SQLNCLI11'
			END
	END;

	-- Tables to store permissions on source and target servers

	CREATE TABLE #Permissions_Source
	(
		PermissionName		SYSNAME
		, ClassDesc			NVARCHAR(60)
		, ObjectIdentifier	SYSNAME NULL
		, StateDesc			NVARCHAR(60)
	);

	CREATE TABLE #Permissions_Target
	(
		PermissionName		SYSNAME
		, ClassDesc			NVARCHAR(60)
		, ObjectIdentifier	SYSNAME NULL
		, StateDesc			NVARCHAR(60)
	);

	-- Loop through all other servers in the availability group.

	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_replicas ar
	INNER JOIN sys.availability_groups ag
		ON ar.group_id = ag.group_id
	WHERE ag.group_id = @AvailabilityGroupId
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor
	INTO @ServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		-- Create a linked server to connect to the AG replica server
		EXEC sys.sp_addlinkedserver
			@server = N'Temp Server For SyncServerPermissionsAcrossAG'
			, @srvproduct = N''
			, @provider = @LinkedServerProvider
			, @datasrc = @ServerName;

		-- Set RPC out to enable use of sp_executesql
		EXEC sys.sp_serveroption
			@server = N'Temp Server For SyncServerPermissionsAcrossAG'
			, @optname = 'RPC out'
			, @optvalue = N'true';
			
		-- To make the PRINT output more easily understandable
		SET @ServerComment =  @CrLf +
			'-- On server: ' + @ServerName + @CrLf;
		
		DECLARE LoginCursor CURSOR LOCAL FAST_FORWARD FOR
		SELECT LoginName
		FROM #Logins_Source;

		OPEN LoginCursor;
		FETCH NEXT FROM LoginCursor
		INTO @SourceLoginName;

		WHILE @@FETCH_STATUS = 0
		BEGIN;
			TRUNCATE TABLE #Permissions_Source;
			TRUNCATE TABLE #Permissions_Target;

			-- Get current permissions from the source server (this server)
			SET @Sql =
				N'INSERT INTO #Permissions_Source' + @CrLf +
				N'(' + @CrLf +
				N'	PermissionName' + @CrLf +
				N'	, ClassDesc' + @CrLf +
				N'	, ObjectIdentifier' + @CrLf +
				N'	, StateDesc' + @CrLf +
				N')' + @CrLf +
				N'SELECT p.permission_name' + @CrLf +
				N'	, p.class_desc' + @CrLf +
				N'	, ObjectIdentifier =' + @CrLf +
				N'		CASE p.class_desc' + @CrLf +
				N'			WHEN ''SERVER'' THEN NULL' + @CrLf +
				N'			WHEN ''SERVER_PRINCIPAL'' THEN sp2.name' + @CrLf +
				N'			WHEN ''ENDPOINT'' THEN ep.name' + @CrLf +
				N'			WHEN ''AVAILABILITY GROUP'' THEN ag.name' + @CrLf +
				N'		END' + @CrLf +
				N'	, p.state_desc' + @CrLf +
				N'FROM sys.server_permissions p' + @CrLf +
				N'INNER JOIN sys.server_principals s' + @CrLf +
				N'	ON s.principal_id = p.grantee_principal_id' + @CrLf +
				N'LEFT JOIN sys.server_principals sp2' + @CrLf +
				N'	ON sp2.principal_id = p.major_id' + @CrLf +
				N'LEFT JOIN sys.endpoints ep' + @CrLf +
				N'	ON ep.endpoint_id = p.major_id' + @CrLf +
				N'LEFT JOIN' + @CrLf +
				N'(' + @CrLf +
				N'	SELECT ar.replica_metadata_id, ag.name' + @CrLf +
				N'	FROM sys.availability_replicas ar' + @CrLf +
				N'	INNER JOIN sys.availability_groups ag' + @CrLf +
				N'		ON ag.group_id = ar.group_id' + @CrLf +
				N') ag ON ag.replica_metadata_id = p.major_id' + @CrLf +
				N'WHERE s.name = N' + QUOTENAME(@SourceLoginName, '''') + @CrLf +
				N'	AND p.state_desc IN (''GRANT'', ''DENY'', ''GRANT_WITH_GRANT_OPTION'');';

			EXEC sys.sp_executesql
				@stmt = @Sql;

			-- Get current permissions from the target server
			DECLARE @RemoteSql NVARCHAR(MAX) = 
				N'SELECT p.permission_name' + @CrLf +
				N'	, p.class_desc' + @CrLf +
				N'	, ObjectIdentifier =' + @CrLf +
				N'		CASE p.class_desc' + @CrLf +
				N'			WHEN ''SERVER'' THEN NULL' + @CrLf +
				N'			WHEN ''SERVER_PRINCIPAL'' THEN sp2.name' + @CrLf +
				N'			WHEN ''ENDPOINT'' THEN ep.name' + @CrLf +
				N'			WHEN ''AVAILABILITY GROUP'' THEN ag.name' + @CrLf +
				N'		END' + @CrLf +
				N'	, p.state_desc' + @CrLf +
				N'FROM sys.server_permissions p' + @CrLf +
				N'INNER JOIN sys.server_principals s' + @CrLf +
				N'	ON s.principal_id = p.grantee_principal_id' + @CrLf +
				N'LEFT JOIN sys.server_principals sp2' + @CrLf +
				N'	ON sp2.principal_id = p.major_id' + @CrLf +
				N'LEFT JOIN sys.endpoints ep' + @CrLf +
				N'	ON ep.endpoint_id = p.major_id' + @CrLf +
				N'LEFT JOIN (' + @CrLf +
				N'	SELECT ar.replica_metadata_id, ag.name' + @CrLf +
				N'	FROM sys.availability_replicas ar' + @CrLf +
				N'	INNER JOIN sys.availability_groups ag' + @CrLf +
				N'		ON ag.group_id = ar.group_id' + @CrLf +
				N') ag ON ag.replica_metadata_id = p.major_id' + @CrLf +
				N'WHERE s.name = N''' + REPLACE(@SourceLoginName, '''', '''''') + N''' ' + @CrLf +
				N'	AND p.state_desc IN (''GRANT'', ''DENY'', ''GRANT_WITH_GRANT_OPTION'');';

			SET @Sql = @CrLf +
				N'INSERT INTO #Permissions_Target' + @CrLf +
				N'(' + @CrLf +
				N'	PermissionName' + @CrLf +
				N'	, ClassDesc' + @CrLf +
				N'	, ObjectIdentifier' + @CrLf +
				N'	, StateDesc' + @CrLf +
				N')' + @CrLf +
				N'SELECT permission_name' + @CrLf +
				N'	, class_desc' + @CrLf +
				N'	, ObjectIdentifier' + @CrLf +
				N'	, state_desc' + @CrLf +
				N'FROM OPENQUERY([Temp Server For SyncServerPermissionsAcrossAG], ''' +
				REPLACE(@RemoteSql, '''', '''''') + N''');';

			EXEC sys.sp_executesql
				@stmt = @Sql;

			-- Revoke permissions on target but not on source
			DECLARE @StateDesc NVARCHAR(60);

			DECLARE RevokePermCursor CURSOR LOCAL FAST_FORWARD FOR
			SELECT t.PermissionName
				, t.ClassDesc
				, t.ObjectIdentifier
				, t.StateDesc
			FROM #Permissions_Target t
			LEFT JOIN #Permissions_Source s
				ON s.PermissionName = t.PermissionName
				AND s.ClassDesc = t.ClassDesc
				AND ISNULL(s.ObjectIdentifier, '') = ISNULL(t.ObjectIdentifier, '')
				AND s.StateDesc = t.StateDesc
			WHERE s.PermissionName IS NULL;

			OPEN RevokePermCursor;

			FETCH NEXT FROM RevokePermCursor
			INTO @PermissionName
				, @ClassDesc
				, @ObjectIdentifier
				, @StateDesc;

			WHILE @@FETCH_STATUS = 0
			BEGIN;
				SET @Sql = @ServerComment +
					N'USE master;' + @CrLf +
					+ @CrLf +
					N'REVOKE ' + @PermissionName +
					CASE
						WHEN @ClassDesc = 'SERVER'
							THEN N''
						WHEN @ClassDesc = 'SERVER_PRINCIPAL'
							THEN N' ON LOGIN::' + QUOTENAME(@ObjectIdentifier)
						WHEN @ClassDesc = 'ENDPOINT'
							THEN N' ON ENDPOINT::' + QUOTENAME(@ObjectIdentifier)
						WHEN @ClassDesc = 'AVAILABILITY GROUP'
							THEN N' ON AVAILABILITY GROUP::' + QUOTENAME(@ObjectIdentifier)
						ELSE N''
					END +
					N' FROM ' + QUOTENAME(@SourceLoginName) + N' CASCADE;' + @CrLf;

				IF @Debug = 1
					PRINT @Sql;

				EXEC [Temp Server For SyncServerPermissionsAcrossAG].master.sys.sp_executesql
					@stmt = @Sql;

				FETCH NEXT FROM RevokePermCursor
				INTO @PermissionName
					, @ClassDesc
					, @ObjectIdentifier
					, @StateDesc;
			END;

			CLOSE RevokePermCursor;
			DEALLOCATE RevokePermCursor;
			
			-- Grant permissions on source but not on target
			
			DECLARE GrantPermCursor CURSOR LOCAL FAST_FORWARD FOR
			SELECT s.PermissionName
				, s.ClassDesc
				, s.ObjectIdentifier
				, s.StateDesc
			FROM #Permissions_Source s
			LEFT JOIN #Permissions_Target t
				ON s.PermissionName = t.PermissionName
				AND s.ClassDesc = t.ClassDesc
				AND ISNULL(s.ObjectIdentifier, '') = ISNULL(t.ObjectIdentifier, '')
				AND s.StateDesc = t.StateDesc
			WHERE t.PermissionName IS NULL;

			OPEN GrantPermCursor;
			FETCH NEXT FROM GrantPermCursor
			INTO @PermissionName
				, @ClassDesc
				, @ObjectIdentifier
				, @StateDesc;

			WHILE @@FETCH_STATUS = 0
			BEGIN;
				SET @Sql = @ServerComment +
					N'USE master;' + @CrLf +
					@CrLf +
					CASE @StateDesc
						WHEN 'GRANT' THEN N'GRANT '
						WHEN 'DENY' THEN N'DENY '
						WHEN 'GRANT_WITH_GRANT_OPTION' THEN N'GRANT '
					END + @PermissionName +
					CASE
						WHEN @ClassDesc = 'SERVER'
							THEN N''
						WHEN @ClassDesc = 'SERVER_PRINCIPAL'
							THEN N' ON LOGIN::' + QUOTENAME(@ObjectIdentifier)
						WHEN @ClassDesc = 'ENDPOINT'
							THEN N' ON ENDPOINT::' + QUOTENAME(@ObjectIdentifier)
						WHEN @ClassDesc = 'AVAILABILITY GROUP'
							THEN N' ON AVAILABILITY GROUP::' + QUOTENAME(@ObjectIdentifier)
						ELSE N''
					END +
					N' TO ' + QUOTENAME(@SourceLoginName) +
					CASE @StateDesc
						WHEN 'GRANT_WITH_GRANT_OPTION' THEN N' WITH GRANT OPTION'
						ELSE N''
					END + N';' + @CrLf;

				IF @Debug = 1
					PRINT @Sql;

				EXEC [Temp Server For SyncServerPermissionsAcrossAG].master.sys.sp_executesql
					@stmt = @Sql;

				FETCH NEXT FROM GrantPermCursor
				INTO @PermissionName
					, @ClassDesc
					, @ObjectIdentifier
					, @StateDesc;
			END;

			CLOSE GrantPermCursor;
			DEALLOCATE GrantPermCursor;

			FETCH NEXT FROM LoginCursor
			INTO @SourceLoginName;
		END;

		CLOSE LoginCursor;
		DEALLOCATE LoginCursor;
		
		-- Drop the linked server
		EXEC sys.sp_dropserver
			@server = N'Temp Server For SyncServerPermissionsAcrossAG';

		FETCH NEXT FROM AvailabilityReplicaCursor
		INTO @ServerName;
	END;

	CLOSE AvailabilityReplicaCursor;
	DEALLOCATE AvailabilityReplicaCursor;
END TRY
BEGIN CATCH
	-- Drop the linked server, if it exists.
	IF EXISTS
	(
		SELECT 1
		FROM sys.servers
		WHERE name = N'Temp Server For SyncServerPermissionsAcrossAG'
	)
	BEGIN
		EXEC sys.sp_dropserver
			@server = N'Temp Server For SyncServerPermissionsAcrossAG';
	END;

	THROW;
END CATCH;


