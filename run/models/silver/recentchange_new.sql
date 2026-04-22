SELECT
	id,
	namespace,
	title,
	comment,
	user,
	bot,
	minor,
	length_new as length,
	uri,
	domain,
	dt,
	patrolled
FROM
	{{ ref('recentchange') }}
WHERE
	"type" = 'new'