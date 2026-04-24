SELECT
	domain,
	log_action,
	strftime(dt, '%Y/%m/%d %H:%M') as minute,
	count(*) as count_actions,
FROM
	{{ ref('recentchange_log') }}
GROUP BY
	domain,
	log_action,
	strftime(dt, '%Y/%m/%d %H:%M')
ORDER BY
	strftime(dt, '%Y/%m/%d %H:%M'),
	domain,
	log_action