IF OBJECT_ID(N'dbo.ecd_overrides', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.ecd_overrides (
    sf_id NVARCHAR(100) NOT NULL,
    metric_key NVARCHAR(200) NOT NULL,
    value NVARCHAR(50) NULL,
    updated_at DATETIME2 NOT NULL CONSTRAINT DF_ecd_overrides_updated_at DEFAULT SYSUTCDATETIME(),
    updated_by NVARCHAR(200) NULL,
    CONSTRAINT PK_ecd_overrides PRIMARY KEY (sf_id, metric_key)
  );
END;
GO

IF OBJECT_ID(N'dbo.audit_events', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.audit_events (
    id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    sf_id NVARCHAR(100) NOT NULL,
    task_id NVARCHAR(100) NULL,
    event_type NVARCHAR(50) NOT NULL,
    metric_key NVARCHAR(200) NULL,
    old_value NVARCHAR(MAX) NULL,
    new_value NVARCHAR(MAX) NULL,
    actor NVARCHAR(200) NULL,
    metadata_json NVARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL CONSTRAINT DF_audit_events_created_at DEFAULT SYSUTCDATETIME()
  );

  CREATE INDEX IX_audit_events_sf_id_created_at ON dbo.audit_events (sf_id, created_at DESC);
END;
GO

IF OBJECT_ID(N'dbo.client_links', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.client_links (
    sf_id NVARCHAR(100) NOT NULL PRIMARY KEY,
    sig NVARCHAR(128) NOT NULL,
    client_url NVARCHAR(500) NOT NULL,
    last_generated_at DATETIME2 NOT NULL CONSTRAINT DF_client_links_generated_at DEFAULT SYSUTCDATETIME()
  );
END;
GO

