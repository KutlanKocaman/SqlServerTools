/*

This script creates the stored procedure GrantPermissionsToProcedure,
and the user-defined table type it accepts as a parameter, called GrantList,
in a database called UtilityDb.

The procedure uses module signing to grant the specified procedure permissions
and role memberships provided in the GrantList.

It's based on the following procedure and script, introduced by Erland Sommarskog
in the article https://www.sommarskog.se/grantperm.html:
* GrantPermsToSP https://www.sommarskog.se/grantperm/GrantPermsToSP.sql.txt
* GrantPermsToSP_server https://www.sommarskog.se/grantperm/GrantPermsToSP_server.sql.txt

This procedure combines the two and adds the ability to grant permissions across databases.

It uses the following naming schema for certificates, certificate users, and certificate logins:
	"Cert for {ProcedureDatabase}.{ProcedureSchema}.{ProcedureName}"
If this is too long, it truncates and uses a hash to ensure the certificate name is unqiue.

It takes a "drop everything then re-apply as requested" approach, meaning that, each time it runs,
it drops all existing certificates, certificate users, and certificate logins
matching this naming schema in all databases, then it re-creates only the ones in the @GrantList.

Tests for this procedure are here:
https://github.com/KutlanKocaman/SqlServerTools/blob/main/Procedures/GrantPermissionsToProcedure.Tests.sql

*/

IF DB_ID('UtilityDb') IS NULL
	CREATE DATABASE UtilityDb;

GO

USE UtilityDb;

IF OBJECT_ID('dbo.GrantPermissionsToProcedure') IS NOT NULL
	DROP PROCEDURE dbo.GrantPermissionsToProcedure;

IF TYPE_ID('dbo.GrantList') IS NOT NULL
	DROP TYPE dbo.GrantList;

CREATE TYPE dbo.GrantList
AS TABLE
(
	GrantScope CHAR(1) NOT NULL -- 'D' = Database, 'S' = Server
	, GrantType CHAR(1) NOT NULL -- 'G' = Grant permission, 'R' = Add role member
	, DatabaseName SYSNAME NOT NULL DEFAULT N'' -- Database name (if GrantScope = 'D')
	, Permission NVARCHAR(400) NOT NULL

	, PRIMARY KEY NONCLUSTERED (GrantScope, GrantType, DatabaseName, Permission)
);

GO

CREATE PROCEDURE dbo.GrantPermissionsToProcedure
(
	@ProcedureDatabase SYSNAME
	, @ProcedureSchema SYSNAME
	, @ProcedureName SYSNAME
	, @GrantList dbo.GrantList READONLY
	, @Debug BIT = 0
)
AS
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @Message NVARCHAR(2048);
DECLARE @InvalidDatabases NVARCHAR(MAX);
DECLARE @DatabasesNotOnline NVARCHAR(MAX);
DECLARE @FullyQualifiedProcedureName NVARCHAR(MAX);
DECLARE @FullyQualifiedProcedureHash NVARCHAR(64);
DECLARE @CertificateNameLong NVARCHAR(MAX);
DECLARE @CertificateName SYSNAME;
DECLARE @CertificateSubject NVARCHAR(64);
DECLARE @CertificatePassword NVARCHAR(128);
-- Execute in each DB as the dbo user,
-- following the pattern in Erland Sommarskog's GrantPermsToSP_server,
-- to protect against malicious a DDL trigger created by database user
DECLARE @ExecuteAsUserDbo NVARCHAR(MAX) = N'EXECUTE AS USER = N''dbo'';';
DECLARE @CertificateId INT;
DECLARE @PublicKey VARBINARY(MAX);
DECLARE @Sql NVARCHAR(MAX);
DECLARE @CrLf NVARCHAR(MAX) = CHAR(13) + CHAR(10);
DECLARE @PreviousDbName SYSNAME;
DECLARE @DatabaseName SYSNAME;
DECLARE @GrantScope CHAR(1);
DECLARE @GrantType CHAR(1);
DECLARE @Permission NVARCHAR(400);
DECLARE @RowCount INT;

BEGIN TRANSACTION;

-- Validate the data in @GrantList.
IF EXISTS
(
	SELECT 1
	FROM @GrantList
	WHERE GrantScope NOT IN ('S','D')
)
BEGIN;
	SET @Message = N'Error in @GrantList: GrantScope must be ' +
		N'either ''D'' (Database) or ''S'' (Server).';

	THROW 50000, @Message, 0;
END;

IF EXISTS
(
	SELECT 1
	FROM @GrantList
	WHERE GrantType NOT IN ('G','R')
)
BEGIN;
	SET @Message = N'Error in @GrantList: GrantType must be either ' +
		N'''G'' (grant permission) or ''R'' (add role member).';

	THROW 50000, @Message, 0;
END;

IF EXISTS
(
	SELECT 1
	FROM @GrantList
	WHERE GrantScope = 'D'
		AND LEN(DatabaseName) = 0
)
BEGIN;
	SET @Message = N'Error in @GrantList: DatabaseName is required ' +
		N'if GrantScope = ''D'' (Database).';

	THROW 50000, @Message, 0;
END;

IF EXISTS
(
	SELECT 1
	FROM @GrantList
	WHERE GrantScope = 'S'
		AND LEN(DatabaseName) > 0
)
BEGIN;
	SET @Message = N'Error in @GrantList: DatabaseName should be blank ' +
		N'if GrantScope = ''S'' (Server).';

	THROW 50000, @Message, 0;
END;

SET @InvalidDatabases =
	STUFF(
		(
			SELECT N',' + DatabaseName
			FROM @GrantList
			WHERE GrantScope = 'D'
				AND DatabaseName NOT IN
				(
					SELECT name
					FROM sys.databases
				)
			ORDER BY DatabaseName
			FOR XML PATH(''), TYPE
		).value('.', 'NVARCHAR(MAX)'),
    1,1,'');

IF LEN(@InvalidDatabases) > 0
BEGIN;
	SET @Message = N'Error in @GrantList: ' +
		N'the following database names are invalid: ' + @InvalidDatabases;

	THROW 50000, @Message, 0;
END;

SET @DatabasesNotOnline =
	STUFF(
		(
			SELECT N',' + DatabaseName
			FROM @GrantList
			WHERE GrantScope = 'D'
				AND DatabaseName NOT IN
				(
					SELECT name
					FROM sys.databases
					WHERE ISNULL(state_desc,'') = 'ONLINE'
				)
			ORDER BY DatabaseName
			FOR XML PATH(''), TYPE
		).value('.', 'NVARCHAR(MAX)'),
    1,1,'');

IF LEN(@DatabasesNotOnline) > 0
BEGIN;
	SET @Message = N'Error in @GrantList: ' +
		N'the following databases are not online: ' + @DatabasesNotOnline;

	THROW 50000, @Message, 0;
END;

-- Set the certificate name.
-- If the full name is longer than 128, then truncate it and use a hash.
SET @FullyQualifiedProcedureName =
	@ProcedureDatabase + N'.' + @ProcedureSchema + N'.' + @ProcedureName;

SET @CertificateNameLong = N'Cert for ' + @FullyQualifiedProcedureName;

IF LEN(REPLACE(@CertificateNameLong, N' ', N'_')) <= 128
BEGIN;
	SET @CertificateName = @CertificateNameLong;
END;
ELSE
BEGIN;
	SET @FullyQualifiedProcedureHash =
		CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', @FullyQualifiedProcedureName), 2);

	SET @CertificateName = LEFT(@CertificateNameLong, 64) + @FullyQualifiedProcedureHash;
END;
	
-- "The subject should be no more than 64 characters long".
-- https://learn.microsoft.com/en-us/sql/t-sql/statements/create-certificate-transact-sql
SET @CertificateSubject = LEFT(@CertificateName, 64);

-- Put an 'a' in the password to ensure it meets the password policy.
-- https://learn.microsoft.com/en-us/sql/relational-databases/security/password-policy
SET @CertificatePassword = CAST(NEWID() AS NVARCHAR(MAX)) + N'a';

-- Get the ID of the existing certificate, by name, in the procedure's database.
SET @Sql = N'USE ' + QUOTENAME(@ProcedureDatabase) + N';' + @CrLf +
	@ExecuteAsUserDbo + @CrLf +
	@CrLf +
	N'SELECT @CertificateId = CERT_ID(QUOTENAME(@CertificateName,''"''));' + @CrLf +
	@CrLf +
	N'REVERT;' + @CrLf;

IF @Debug = 1
	PRINT @Sql;
	
IF @Sql IS NULL
	THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 1;

EXEC sys.sp_executesql
	@stmt = @Sql
	, @params = N'@CertificateName SYSNAME
		, @CertificateId INT OUTPUT'
	, @CertificateName = @CertificateName
	, @CertificateId = @CertificateId OUTPUT;

-- If the stored procedure is signed by this certificate, remove the signature.
SET @Sql = @CrLf +
	N'USE ' + QUOTENAME(@ProcedureDatabase) + N';' + @CrLf +
	@ExecuteAsUserDbo + @CrLf +
	@CrLf +
	N'IF EXISTS' + @CrLf +
	N'(' + @CrLf +
	N'	SELECT 1' + @CrLf +
	N'	FROM sys.crypt_properties' + @CrLf +
	N'	WHERE major_id = OBJECT_ID(''' +
			QUOTENAME(@ProcedureSchema) + N'.' + QUOTENAME(@ProcedureName) +
			N''')' + @CrLf +
	N'		AND class_desc = ''OBJECT_OR_COLUMN''' + @CrLf +
	N'		AND crypt_type_desc = ''SIGNATURE BY CERTIFICATE''' + @CrLf +
	N'		AND thumbprint =' + @CrLf +
	N'			(' + @CrLf +
	N'				SELECT thumbprint' + @CrLf +
	N'				FROM sys.certificates' + @CrLf +
	N'				WHERE certificate_id = @CertificateId' + @CrLf +
	N'			)' + @CrLf +
	N')' + @CrLf +
	N'BEGIN;' + @CrLf +
	N'	DROP SIGNATURE FROM ' +
		QUOTENAME(@ProcedureSchema) + N'.' + QUOTENAME(@ProcedureName) + @CrLf +
	N'	BY CERTIFICATE ' + QUOTENAME(@CertificateName) + N';' + @CrLf +
	N'END;' + @CrLf +
	@CrLf +
	N'REVERT;' + @CrLf;

IF @Debug = 1
	PRINT @Sql;
	
IF @Sql IS NULL
	THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 2;

EXEC sys.sp_executesql
	@stmt = @Sql
	, @params = N'@ProcedureSchema SYSNAME
		, @ProcedureName SYSNAME
		, @CertificateId INT'
	, @ProcedureSchema = @ProcedureSchema
	, @ProcedureName = @ProcedureName
	, @CertificateId = @CertificateId;

-- If a certificate login exists then drop it.
IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @CertificateName)
BEGIN;
	SET @Sql = @CrLf + N'DROP LOGIN ' + QUOTENAME(@CertificateName);
	
	IF @Debug = 1
		PRINT @Sql;
		
	IF @Sql IS NULL
		THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 3;

	EXEC sys.sp_executesql @stmt = @Sql;
END;

-- Loop through all online databases to delete existing certificates and certificate users.
DECLARE DatabaseCursor CURSOR FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE state_desc = 'ONLINE';

OPEN DatabaseCursor;
FETCH NEXT FROM DatabaseCursor INTO @DatabaseName;

WHILE @@FETCH_STATUS = 0
BEGIN;
	-- If a certificate user exists whose name matches the certificate name then delete it.
	SET @Sql = @CrLf +
		N'SELECT @RowCount = COUNT(*)' + @CrLf +
		N'FROM ' + QUOTENAME(@DatabaseName) + N'.sys.database_principals' + @CrLf +
		N'WHERE name = @CertificateName;' + @CrLf;
			
	IF @Debug = 1
		PRINT @Sql;
		
	IF @Sql IS NULL
		THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 4;

	EXEC sys.sp_executesql
		@stmt = @Sql
		, @params = N'@CertificateName SYSNAME
			, @RowCount INT OUTPUT'
		, @CertificateName = @CertificateName
		, @RowCount = @RowCount OUTPUT;

	IF @RowCount = 1
	BEGIN;
		SET @Sql = @CrLf +
			N'USE ' + QUOTENAME(@DatabaseName) + N';' + @CrLf +
			@ExecuteAsUserDbo + @CrLf +
			N'' + @CrLf +
			N'DROP USER ' + QUOTENAME(@CertificateName) + N';' + @CrLf +
			@CrLf +
			N'REVERT;' + @CrLf;

		IF @Debug = 1
			PRINT @Sql;
			
		IF @Sql IS NULL
			THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 5;
			
		EXEC sys.sp_executesql @stmt = @Sql;
	END;
	
	-- If a certificate exists whose name matches the certificate name then delete it.
	SET @Sql = @CrLf +
		N'SELECT @RowCount = COUNT(*)' + @CrLf +
		N'FROM ' + QUOTENAME(@DatabaseName) + N'.sys.certificates' + @CrLf +
		N'WHERE name = @CertificateName;' + @CrLf;
			
	IF @Debug = 1
		PRINT @Sql;
		
	IF @Sql IS NULL
		THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 6;

	EXEC sys.sp_executesql
		@stmt = @Sql
		, @params = N'@CertificateName SYSNAME
			, @RowCount INT OUTPUT'
		, @CertificateName = @CertificateName
		, @RowCount = @RowCount OUTPUT;

	IF @RowCount = 1
	BEGIN;
		SET @Sql = @CrLf +
			N'USE ' + QUOTENAME(@DatabaseName) + N';' + @CrLf +
			@ExecuteAsUserDbo + @CrLf +
			N'' + @CrLf +
			N'DROP CERTIFICATE ' + QUOTENAME(@CertificateName) + N';' + @CrLf +
			@CrLf +
			N'REVERT;' + @CrLf;
				
		IF @Debug = 1
			PRINT @Sql;
			
		IF @Sql IS NULL
			THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 7;
			
		EXEC sys.sp_executesql @stmt = @Sql;
	END;

	FETCH NEXT FROM DatabaseCursor INTO @DatabaseName;
END;

CLOSE DatabaseCursor;
DEALLOCATE DatabaseCursor;

-- We're done deleting existing certificates and users.
-- If no permissions were granted then we can RETURN now.
-- The rest of the code below is to add the requested permissions.
IF NOT EXISTS (SELECT 1 FROM @GrantList)
BEGIN;
	COMMIT TRANSACTION;

	RETURN;
END;

-- Create the certificate.
-- Expired certificates still work for module signing but they can give you warnings,
-- so make the expiry date 31 December 9999.
SET @Sql = @CrLf +
	N'USE ' + QUOTENAME(@ProcedureDatabase) + N';' + @CrLf +
	@ExecuteAsUserDbo + @CrLf +
	@CrLf +
	N'CREATE CERTIFICATE ' + QUOTENAME(@CertificateName) + @CrLf +
	N'ENCRYPTION BY PASSWORD = ' + QUOTENAME(@CertificatePassword, '''') + @CrLf +
	N'WITH SUBJECT = ''' + QUOTENAME(@CertificateSubject,'"') + N'''' + @CrLf +
	N'	, EXPIRY_DATE = ''9999-12-31T00:00:00'';' + @CrLf +
	@CrLf +
	N'REVERT;' + @CrLf;

IF @Debug = 1
	PRINT @Sql;
	
IF @Sql IS NULL
	THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 8;
	
EXEC sys.sp_executesql @stmt = @Sql;

-- Sign the procedure with the certificate.
SET @Sql = @CrLf +
	N'USE ' + QUOTENAME(@ProcedureDatabase) + N';' + @CrLf +
	@ExecuteAsUserDbo + @CrLf +
	@CrLf +
	N'ADD SIGNATURE TO ' + QUOTENAME(@ProcedureSchema) + N'.' + QUOTENAME(@ProcedureName) + @CrLf +
	N'BY CERTIFICATE ' + QUOTENAME(@CertificateName) + @CrLf +
	N'WITH PASSWORD = ' + QUOTENAME(@CertificatePassword, '''') + N';' + @CrLf +
	@CrLf +
	N'REVERT;' + @CrLf;

IF @Debug = 1
	PRINT @Sql;
	
IF @Sql IS NULL
	THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 9;
	
EXEC sys.sp_executesql @stmt = @Sql;

-- Drop the private key. It's not needed anymore and we don't want it lying around.
SET @Sql = @CrLf +
	N'USE ' + QUOTENAME(@ProcedureDatabase) + N';' + @CrLf +
	@ExecuteAsUserDbo + @CrLf +
	@CrLf +
	N'ALTER CERTIFICATE ' + QUOTENAME(@CertificateName) + @CrLf +
	N'REMOVE PRIVATE KEY;' + @CrLf +
	@CrLf +
	N'REVERT;' + @CrLf;

IF @Debug = 1
	PRINT @Sql;
	
IF @Sql IS NULL
	THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 10;
	
EXEC sys.sp_executesql @stmt = @Sql;

-- Get the certificate's public key.
SET @Sql = @CrLf +
	N'USE ' + QUOTENAME(@ProcedureDatabase) + N';' + @CrLf +
	@ExecuteAsUserDbo + @CrLf +
	@CrLf +
	N'SELECT @CertificateId = CERT_ID(QUOTENAME(@CertificateName,''"''));' + @CrLf +
	N'SELECT @PublicKey = CERTENCODED(@CertificateId);' + @CrLf +
	@CrLf +
	N'REVERT;' + @CrLf;

IF @Debug = 1
	PRINT @Sql;
	
IF @Sql IS NULL
	THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 11;

EXEC sys.sp_executesql
	@stmt = @Sql
	, @params = N'@CertificateName SYSNAME
		, @CertificateId INT OUTPUT
		, @PublicKey VARBINARY(MAX) OUTPUT'
	, @CertificateName = @CertificateName
	, @CertificateId = @CertificateId OUTPUT
	, @PublicKey = @PublicKey OUTPUT;
	
-- Loop through all permissions to grant.
DECLARE GrantListCursor CURSOR FAST_FORWARD FOR
SELECT GrantScope
	, GrantType
	, DatabaseName
	, Permission
FROM @GrantList
ORDER BY DatabaseName;

OPEN GrantListCursor;

FETCH NEXT FROM GrantListCursor
INTO @GrantScope
	, @GrantType
	, @DatabaseName
	, @Permission;

WHILE @@FETCH_STATUS = 0
BEGIN;
	IF @GrantScope = 'S' AND @DatabaseName = N''
		SET @DatabaseName = 'master';
		
	-- If this is the first permission to grant in this database
	-- then create a certificate.
	IF (@PreviousDbName IS NULL OR @PreviousDbName <> @DatabaseName)
	BEGIN;
		SET @PreviousDbName = @DatabaseName;

		-- There will already be a certificate in the @ProcedureDatabase.
		-- Don't try to re-create it.
		IF @DatabaseName <> @ProcedureDatabase
		BEGIN;
			SET @Sql = @CrLf +
				N'USE ' + QUOTENAME(@DatabaseName) + N';' + @CrLf +
				@ExecuteAsUserDbo + @CrLf +
				@CrLf +
				N'CREATE CERTIFICATE ' + QUOTENAME(@CertificateName) + @CrLf +
				N'FROM BINARY = ' + CONVERT(VARCHAR(MAX), @PublicKey, 1) + @CrLf +
				@CrLf +
				N'REVERT;' + @CrLf;

			IF @Debug = 1
				PRINT @Sql;
				
			IF @Sql IS NULL
				THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 12;
				
			EXEC sys.sp_executesql @stmt = @Sql;
		END;
	END;

	IF @GrantScope = 'D' -- Database-scope
	BEGIN;
		SET @Sql = @CrLf +
			N'SELECT @RowCount = COUNT(*) ' + @CrLf +
			N'FROM ' + QUOTENAME(@DatabaseName) + N'.sys.database_principals ' + @CrLf +
			N'WHERE name = @CertificateName;' + @CrLf;
		
		IF @Debug = 1
			PRINT @Sql;

		EXEC sys.sp_executesql
			@stmt = @Sql
			, @params = N'@CertificateName SYSNAME
				, @RowCount INT OUTPUT'
			, @CertificateName = @CertificateName
			, @RowCount = @RowCount OUTPUT;

		IF @RowCount = 0
		BEGIN;
			SET @Sql = @CrLf +
				N'USE ' + QUOTENAME(@DatabaseName) + N';' + @CrLf +
				@ExecuteAsUserDbo + @CrLf +
				@CrLf +
				N'CREATE USER ' + QUOTENAME(@CertificateName) + @CrLf +
				N'FROM CERTIFICATE ' + QUOTENAME(@CertificateName) + @CrLf +
				@CrLf +
				N'REVERT;' + @CrLf;
			
			IF @Debug = 1
				PRINT @Sql;
			
			IF @Sql IS NULL
				THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 13;
			
			EXEC sys.sp_executesql @stmt = @Sql;
		END;
	END;
	ELSE IF @GrantScope = 'S' -- Server-scope
	BEGIN;
		IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @CertificateName)
		BEGIN;
			-- Create the certificate user.
			SET @Sql = @CrLf +
				N'USE ' + QUOTENAME(@DatabaseName) + N';' + @CrLf +
				@CrLf +
				N'CREATE LOGIN ' + QUOTENAME(@CertificateName) + @CrLf +
				N'FROM CERTIFICATE ' + QUOTENAME(@CertificateName) + @CrLf;
			
			IF @Debug = 1
				PRINT @Sql;
			
			IF @Sql IS NULL
				THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 14;
			
			EXEC sys.sp_executesql @stmt = @Sql;
		END;
	END;

	-- Grant the permission.
	SET @Sql = @CrLf +
		N'USE ' + QUOTENAME(@DatabaseName) + N';' + @CrLf;

	IF @GrantType = 'G' -- Grant
	BEGIN;
		IF @GrantScope = 'D' -- Database-scope
			SET @Sql += @ExecuteAsUserDbo + @CrLf;

		SET @Sql += @CrLf +
			N'GRANT ' + @Permission + @CrLf +
			N'TO ' + QUOTENAME(@CertificateName) + N';' + @CrLf;

		IF @GrantScope = 'D' -- Database-scope
			SET @Sql += + @CrLf + N'REVERT;' + @CrLf;
	END;
	ELSE IF @GrantType = 'R' -- Role
	BEGIN;
		IF @GrantScope = 'D' -- Database-scope
		BEGIN;
			SET @Sql += @CrLf +
				@ExecuteAsUserDbo + @CrLf +
				N'ALTER ROLE ' + @Permission + @CrLf +
				N'ADD MEMBER ' + QUOTENAME(@CertificateName) + N';' + @CrLf +
				@CrLf +
				N'REVERT;' + @CrLf;
		END;
		ELSE IF @GrantScope = 'S' -- Server-scope
		BEGIN;
			SET @Sql += @CrLf +
				N'ALTER SERVER ROLE ' + @Permission + @CrLf +
				N'ADD MEMBER ' + QUOTENAME(@CertificateName) + N';' + @CrLf;
		END;
	END;

	IF @Debug = 1
		PRINT @Sql;
		
	IF @Sql IS NULL
		THROW 50000, 'Dynamic @Sql string is unexpectedly NULL.', 15;
		
	EXEC sys.sp_executesql @stmt = @Sql;
	
	FETCH NEXT FROM GrantListCursor
	INTO @GrantScope
		, @GrantType
		, @DatabaseName
		, @Permission;
END;

CLOSE GrantListCursor;
DEALLOCATE GrantListCursor;

COMMIT TRANSACTION;