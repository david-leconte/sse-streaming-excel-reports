SELECT
	id,
    namespace,
	title,
	comment,
	user,
	bot,
	minor,
	(revision__new - revision__old) AS revision__global_offset,
	(length__new - length__old) AS length__difference,
	meta__uri,
	meta__domain,
	meta__dt,
	patrolled
FROM
	{{ ref('recentchange_all') }}
WHERE
	"type" = 'edit'