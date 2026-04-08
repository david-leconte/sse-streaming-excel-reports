SELECT
	id,
	namespace,
	title,
	comment,
	user,
	bot,
	minor,
	length__new,
	meta__uri,
	meta__domain,
	meta__dt,
	patrolled
FROM
	{{ ref('recentchange_all') }}
WHERE
	"type" = 'new'