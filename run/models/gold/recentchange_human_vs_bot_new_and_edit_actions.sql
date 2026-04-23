WITH union_new_and_edit_actions AS (
	SELECT
		strftime(dt, '%Y/%m/%d %H:%M') as minute,
		bot
	FROM
		{{ ref('recentchange_edit') }}
	UNION ALL
	SELECT
		strftime(dt, '%Y/%m/%d %H:%M') as minute,
		bot
	FROM
		{{ ref('recentchange_new') }}
)
SELECT
	minute,
	bot,
	count(*) as count_actions,
FROM
	union_new_and_edit_actions
GROUP BY
	bot,
	minute
ORDER BY
	minute,
	bot