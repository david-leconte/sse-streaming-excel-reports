SELECT
	CAST(event.id AS BIGINT) AS id,
	json_extract_string(event, '$.namespace') AS namespace,
	json_extract_string(event, '$.type') AS type,
	json_extract_string(event, '$.title') AS title,
	json_extract_string(event, '$.comment') AS comment,
	json_extract_string(event, '$.user') AS user,
	CAST(event.bot AS BOOLEAN) AS bot,
	CAST(event.minor AS BOOLEAN) AS minor,
	CAST(event.revision.new AS BIGINT) AS revision_new,
	CAST(event.revision.old AS BIGINT) AS revision_old,
	CAST(event.length.new AS BIGINT) AS length_new,
	CAST(event.length.old AS BIGINT) AS length_old,
	json_extract_string(event, '$.meta.uri') AS uri,
	json_extract_string(event, '$.meta.domain') AS domain,
	CAST(event.meta.dt AS TIMESTAMP) AS dt,
	CAST(event.patrolled AS BOOLEAN) AS patrolled,
	json_extract_string(event, '$.log_type') AS log_type,
	json_extract_string(event, '$.log_action') AS log_action,
	event.log_params
FROM
	{{ source('bronze', 'recentchange') }}