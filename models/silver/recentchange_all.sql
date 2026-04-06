SELECT
	* EXCLUDE(
		"$schema", 
		title_url,
		timestamp,
        parsedcomment,
		notify_url, 
		server_url, 
		server_name,
		wiki,
		server_script_path,
		log_id,
        log_action_comment,
	)
FROM
	{{ source('bronze', 'recentchange') }}