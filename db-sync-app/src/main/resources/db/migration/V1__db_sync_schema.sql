IF OBJECT_ID('accepted_t', 'U') IS NULL
CREATE TABLE accepted_t (
    id NVARCHAR(128) PRIMARY KEY,
    pid NVARCHAR(128) NOT NULL,
    ref NVARCHAR(128) NOT NULL,
    cancel BIT NOT NULL,
    q BIGINT NOT NULL,
    source_topic NVARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    source_timestamp BIGINT NOT NULL,
    event_id NVARCHAR(255) NOT NULL UNIQUE
);

IF OBJECT_ID('accepted_s', 'U') IS NULL
CREATE TABLE accepted_s (
    id NVARCHAR(128) PRIMARY KEY,
    pid NVARCHAR(128) NOT NULL,
    q BIGINT NOT NULL,
    source_topic NVARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    source_timestamp BIGINT NOT NULL,
    event_id NVARCHAR(255) NOT NULL UNIQUE
);

IF OBJECT_ID('generated_ts', 'U') IS NULL
CREATE TABLE generated_ts (
    id NVARCHAR(128) PRIMARY KEY,
    pid NVARCHAR(128) NOT NULL,
    q BIGINT NOT NULL,
    source_topic NVARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    source_timestamp BIGINT NOT NULL,
    event_id NVARCHAR(255) NOT NULL UNIQUE
);

IF OBJECT_ID('unprocessed_t_state', 'U') IS NULL
CREATE TABLE unprocessed_t_state (
    pid NVARCHAR(128) PRIMARY KEY,
    t_id NVARCHAR(128) NOT NULL,
    ref NVARCHAR(128) NOT NULL,
    cancel BIT NOT NULL,
    q BIGINT NOT NULL,
    source_topic NVARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    source_timestamp BIGINT NOT NULL,
    event_id NVARCHAR(255) NOT NULL
);

IF OBJECT_ID('unprocessed_s_state', 'U') IS NULL
CREATE TABLE unprocessed_s_state (
    pid NVARCHAR(128) PRIMARY KEY,
    s_id NVARCHAR(128) NOT NULL,
    q BIGINT NOT NULL,
    source_topic NVARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    source_timestamp BIGINT NOT NULL,
    event_id NVARCHAR(255) NOT NULL
);

IF OBJECT_ID('kafka_consumer_offsets', 'U') IS NULL
CREATE TABLE kafka_consumer_offsets (
    consumer_group NVARCHAR(255) NOT NULL,
    topic NVARCHAR(255) NOT NULL,
    partition_id INT NOT NULL,
    next_offset BIGINT NOT NULL,
    updated_at DATETIME2 NOT NULL,
    PRIMARY KEY (consumer_group, topic, partition_id)
);
