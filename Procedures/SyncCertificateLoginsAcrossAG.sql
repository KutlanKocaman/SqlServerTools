/*

This procedure syncs all certificate-mapped logins matching the
@LoginNamePattern specified, and their associated certificates,
across the availability group specified in @AvailabilityGroupName.

The default @LoginNamePattern is '%', meaning all certificate logins will be synced.

Here's the basic logic:
If a login exists on this server but not the target server, it's created on the target.
	If necessary, the certificate on the target server is dropped and re-created to achieve this.
Else if a login exists on the target server but not this server, it's dropped on the target.
	If @DeleteCertificateWithLogin = 1, then the certificate behind it is also dropped.
Else if a login exists on this server and the target, but the certificate public keys don't match,
then both the certificate and the login are dropped and re-created on the target server.

This procedure can't be used within a transaction because it uses sp_addlinkedserver.

Tests for this procedure are here:
https://github.com/KutlanKocaman/SqlServerTools/blob/main/Procedures/SyncCertificateLoginsAcrossAG.Tests.sql

*/

IF DB_ID('UtilityDb') IS NULL
	CREATE DATABASE UtilityDb;

GO

USE UtilityDb;

IF OBJECT_ID('dbo.SyncCertificateLoginsAcrossAG') IS NOT NULL
	DROP PROCEDURE dbo.SyncCertificateLoginsAcrossAG;

GO

CREATE PROCEDURE dbo.SyncCertificateLoginsAcrossAG
(
	@AvailabilityGroupName SYSNAME
	, @Debug BIT = 0 -- 1 = PRINT all dynamic SQL executed
	, @LoginNamePattern NVARCHAR(MAX) = N'%'
	, @DeleteCertificateWithLogin BIT = 0
		-- If @DeleteCertificateWithLogin = 1 then,
		-- when deleting a certificate-mapped login on the target
		-- then also delete the certificate from which the login was created.
		-- e.g. ThisServer: login1 mapped to cert1
			-- OtherServer: login1 mapped to cert2
			-- EXEC SyncCertificateLoginsAcrossAG @LoginNamePattern = 'login1'
			-- When @DeleteCertificateWithLogin = 1 then DROP cert2
			-- else don't drop cert2
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
	DECLARE @Action VARCHAR(100);
	DECLARE @SourceLoginName SYSNAME;
	DECLARE @TargetLoginName SYSNAME;
	DECLARE @SourceCertificateName SYSNAME;
	DECLARE @SourceCertificatePublicKey VARBINARY(MAX);
	DECLARE @TargetCertificateName SYSNAME;
	DECLARE @TargetCertificatePublicKey VARBINARY(MAX);
	DECLARE @CertificateMatchingSourceNameExistsOnTarget INT;
	DECLARE @CertificateHasCorrectPublicKey INT;

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

	-- Make sure it's not a contained availability group (SQL Server 2022 onwards).

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
			THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 1;

		EXEC sys.sp_executesql
			@Sql
			, N'@AvailabilityGroupId SYSNAME
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

	-- Get the certificate-mapped logins to sync across the availability group.

	CREATE TABLE #CertificateLogins_Source
	(
		LoginName SYSNAME
		, CertificateName SYSNAME
		, CertificatePublicKey VARBINARY(MAX)
	);

	SET @Sql = @CrLf +
		N'-- on the current server.' + @CrLf +
		N'USE master;' + @CrLf +
		@CrLf +
		N'INSERT INTO #CertificateLogins_Source' + @CrLf +
		N'(' + @CrLf +
		N'	LoginName' + @CrLf +
		N'	, CertificateName' + @CrLf +
		N'	, CertificatePublicKey' + @CrLf +
		N')' + @CrLf +
		N'SELECT LoginName = sp.name' + @CrLf +
		N'	, CertificateName = c.name' + @CrLf +
		N'	, CertificatePublicKey = CERTENCODED(c.certificate_id)' + @CrLf +
		N'FROM sys.server_principals sp' + @CrLf +
		N'INNER JOIN sys.certificates c ON sp.sid = c.sid' + @CrLf +
		N'WHERE sp.type_desc = N''CERTIFICATE_MAPPED_LOGIN''' + @CrLf +
		N'	AND sp.name LIKE @LoginNamePattern;' + @CrLf;
	
	IF @Debug = 1
		PRINT @Sql;
	
	IF @Sql IS NULL
		THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 1;

	EXEC sys.sp_executesql
		@Sql,
		N'@LoginNamePattern NVARCHAR(MAX)',
		@LoginNamePattern = @LoginNamePattern;

	CREATE TABLE #CertificateLogins_Target
	(
		LoginName SYSNAME
		, CertificateName SYSNAME
		, CertificatePublicKey VARBINARY(MAX)
	);
	
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

	-- Loop through all other servers in the availability group.

	DECLARE AvailabilityReplicaCursor CURSOR LOCAL FAST_FORWARD FOR
	SELECT ar.replica_server_name
	FROM sys.availability_replicas ar
	INNER JOIN sys.availability_groups ag ON ar.group_id = ag.group_id
	WHERE ag.group_id = @AvailabilityGroupId
		AND ar.replica_server_name <> @@SERVERNAME
	ORDER BY ar.replica_server_name;

	OPEN AvailabilityReplicaCursor;
	FETCH NEXT FROM AvailabilityReplicaCursor INTO @ServerName;

	WHILE @@FETCH_STATUS = 0
	BEGIN;
		-- Create a linked server to connect to the AG replica server.
		EXEC sys.sp_addlinkedserver
			@server = N'Temp Server For SyncCertificateLoginsAcrossAG'
			, @srvproduct = N''
			, @provider = @LinkedServerProvider
			, @datasrc = @ServerName;

		-- Set RPC out to enable use of sp_executesql.
		EXEC sys.sp_serveroption
			@server = N'Temp Server For SyncCertificateLoginsAcrossAG'
			, @optname = 'RPC out'
			, @optvalue = N'true';
			
		-- To make the PRINT output more easily understandable.
		SET @ServerComment =  @CrLf +
			'-- On server: ' + @ServerName + @CrLf;
		
		TRUNCATE TABLE #CertificateLogins_Target;

		-- Get the certificate logins matching the @LoginNamePattern on the target server.
		-- Unfortunately, we have to run this whole thing as Dynamic SQL,
		-- because OPENQUERY doesn't accept variables as arguments.
		SET @Sql = @ServerComment +
			N'USE master;' + @CrLf +
			@CrLf +
			N'INSERT INTO #CertificateLogins_Target' + @CrLf +
			N'(' + @CrLf +
			N'	LoginName' + @CrLf +
			N'	, CertificateName' + @CrLf +
			N'	, CertificatePublicKey' + @CrLf +
			N') ' + @CrLf +
			N'SELECT LoginName' + @CrLf +
			N'	, CertificateName' + @CrLf +
			N'	, CertificatePublicKey' + @CrLf +
			N'FROM OPENQUERY' + @CrLf +
			N'(' + @CrLf +
			N'	[Temp Server For SyncCertificateLoginsAcrossAG]' + @CrLf +
			N'	, ''SELECT LoginName = sp.name' + @CrLf +
			N'		, CertificateName = c.name' + @CrLf +
			N'		, CertificatePublicKey = CERTENCODED(c.certificate_id)' + @CrLf +
			N'	FROM sys.server_principals sp ' + @CrLf +
			N'	INNER JOIN sys.certificates c ON sp.sid = c.sid ' + @CrLf +
			N'	WHERE sp.type_desc = ''''CERTIFICATE_MAPPED_LOGIN'''' ' + @CrLf +
			N'		AND sp.name LIKE '''''
						+ REPLACE(@LoginNamePattern, '''', '''''') + '''''''' + @CrLf +
			N');' + @CrLf;

		IF @Debug = 1
			PRINT @Sql;
			
		IF @Sql IS NULL
			THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 2;

		EXEC sys.sp_executesql @Sql;

		-- Loop through the logins. Sync the target to match this server.

		DECLARE LoginsCursor CURSOR LOCAL FAST_FORWARD FOR
		SELECT SourceLoginName = s.LoginName
			, TargetLoginName = t.LoginName
		FROM #CertificateLogins_Source s
		FULL JOIN #CertificateLogins_Target t ON s.LoginName = t.LoginName
		WHERE s.LoginName IS NULL
			OR t.LoginName IS NULL
			OR s.CertificatePublicKey <> t.CertificatePublicKey
		ORDER BY s.LoginName;

		OPEN LoginsCursor;

		FETCH NEXT FROM LoginsCursor
		INTO @SourceLoginName
			, @TargetLoginName;

		WHILE @@FETCH_STATUS = 0
		BEGIN;
			SET @Action =
				CASE
					WHEN @SourceLoginName IS NULL THEN 'DELETE'
					WHEN @TargetLoginName IS NULL THEN 'CREATE'
					ELSE 'RECREATE' -- s.CertificatePublicKey <> t.CertificatePublicKey
				END;

			-- Get the source data.

			SELECT @SourceCertificateName = NULL
				, @SourceCertificatePublicKey = NULL;

			SELECT @SourceCertificateName = CertificateName
				, @SourceCertificatePublicKey = CertificatePublicKey
			FROM #CertificateLogins_Source
			WHERE LoginName = @SourceLoginName;
			
			-- Get the target data.

			SELECT @TargetCertificateName = NULL;

			SELECT @TargetCertificateName = CertificateName
				, @TargetCertificatePublicKey = CertificatePublicKey
			FROM #CertificateLogins_Target
			WHERE LoginName = @TargetLoginName;

			IF @Action IN ('DELETE', 'RECREATE')
			BEGIN;
				-- Drop the login.

				SET @Sql = @ServerComment +
					N'USE master;' + @CrLf +
					@CrLf +
					N'DROP LOGIN ' + QUOTENAME(@TargetLoginName) + N';' + @CrLf;
					
				IF @Debug = 1
					PRINT @Sql;
					
				IF @Sql IS NULL
					THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 3;
					
				EXEC [Temp Server For SyncCertificateLoginsAcrossAG]
					.master.sys.sp_executesql @Sql;

				-- If @DeleteCertificateWithLogin = 1
				-- then drop the certificate behind the login we just deleted.
				
				IF @TargetCertificateName IS NOT NULL
				AND @DeleteCertificateWithLogin = 1
				BEGIN;
					SET @Sql = @ServerComment +
						N'USE master;' + @CrLf +
						@CrLf +
						N'DROP CERTIFICATE ' + QUOTENAME(@TargetCertificateName) + N';' + @CrLf;

					IF @Debug = 1
						PRINT @Sql;
					
					IF @Sql IS NULL
						THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 4;

					EXEC [Temp Server For SyncCertificateLoginsAcrossAG]
						.master.sys.sp_executesql @Sql;
				END;
			END;
			
			IF @Action IN ('CREATE', 'RECREATE')
			BEGIN;
				-- We know we need to create or recreate the login,
				-- but we don't know whether we need to create or recreate the certificate.
				-- So, find out if the certificate exists on the target,
				-- and whether it has the correct public key.

				SET @Sql = @ServerComment +
					N'USE master;' + @CrLf +
					@CrLf +
					N'SELECT @CertificateMatchingSourceNameExistsOnTarget = COUNT(*)' + @CrLf +
					N'	, @CertificateHasCorrectPublicKey = SUM(' + @CrLf +
					N'		CASE' + @CrLf +
					N'			WHEN CERTENCODED(certificate_id) = ' +
									CONVERT(VARCHAR(MAX), @SourceCertificatePublicKey, 1) + @CrLf +
					N'				THEN 1' + @CrLf +
					N'			ELSE 0' + @CrLf +
					N'		END)' + @CrLf +
					N'FROM sys.certificates' + @CrLf +
					N'WHERE name = ' + QUOTENAME(@SourceCertificateName, '''') + N';' + @CrLf;
					
				IF @Debug = 1
					PRINT @Sql;
					
				IF @Sql IS NULL
					THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 5;

				EXEC [Temp Server For SyncCertificateLoginsAcrossAG]
					.master.sys.sp_executesql
					@Sql
					, N'@CertificateMatchingSourceNameExistsOnTarget INT OUTPUT
						, @CertificateHasCorrectPublicKey INT OUTPUT'
					, @CertificateMatchingSourceNameExistsOnTarget =
						@CertificateMatchingSourceNameExistsOnTarget OUTPUT
					, @CertificateHasCorrectPublicKey = @CertificateHasCorrectPublicKey OUTPUT;

				-- If the certificate matching the @SourceCertificateName exists,
				-- and it doesn't have the correct public key, then drop it.

				IF @CertificateMatchingSourceNameExistsOnTarget = 1
				AND @CertificateHasCorrectPublicKey = 0
				BEGIN;
					SET @Sql = @ServerComment +
						N'USE master;' + @CrLf +
						@CrLf +
						N'DROP CERTIFICATE ' + QUOTENAME(@SourceCertificateName) + N';' + @CrLf;
					
					IF @Debug = 1
						PRINT @Sql;
					
					IF @Sql IS NULL
						THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 5;

					EXEC [Temp Server For SyncCertificateLoginsAcrossAG]
						.master.sys.sp_executesql @Sql;
				END;

				-- If the certificate doesn't already exist on the target
				-- with the correct public key, then create it.

				IF @CertificateMatchingSourceNameExistsOnTarget = 0
				OR @CertificateHasCorrectPublicKey = 0
				BEGIN;
					SET @Sql = @ServerComment +
						N'USE master;' + @CrLf +
						@CrLf +
						N'CREATE CERTIFICATE ' + QUOTENAME(@SourceCertificateName) + @CrLf +
						N'FROM BINARY =' +
							CONVERT(VARCHAR(MAX), @SourceCertificatePublicKey, 1) + N';' + @CrLf;

					IF @Debug = 1
						PRINT @Sql;
					
					IF @Sql IS NULL
						THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 5;

					EXEC [Temp Server For SyncCertificateLoginsAcrossAG]
						.master.sys.sp_executesql @Sql;
				END;

				-- Create a login from the certificate.

				SET @Sql = @ServerComment +
					N'USE master;' + @CrLf +
					@CrLf +
					N'CREATE LOGIN ' + QUOTENAME(@SourceLoginName) + @CrLf +
					N'FROM CERTIFICATE ' + QUOTENAME(@SourceCertificateName) + N';' + @CrLf;

				IF @Debug = 1
					PRINT @Sql;
					
				IF @Sql IS NULL
					THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 6;

				EXEC [Temp Server For SyncCertificateLoginsAcrossAG]
					.master.sys.sp_executesql @Sql;
			END;
			
			FETCH NEXT FROM LoginsCursor
			INTO @SourceLoginName
				, @TargetLoginName;
		END;

		CLOSE LoginsCursor;
		DEALLOCATE LoginsCursor;

		-- Drop the linked server
		EXEC sys.sp_dropserver
			@server = N'Temp Server For SyncCertificateLoginsAcrossAG';

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
		WHERE name = N'Temp Server For SyncCertificateLoginsAcrossAG'
	)
	BEGIN
		EXEC sys.sp_dropserver
			@server = N'Temp Server For SyncCertificateLoginsAcrossAG';
	END;

	THROW;
END CATCH;


