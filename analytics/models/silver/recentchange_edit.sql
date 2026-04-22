SELECT
	id,
    namespace,
	title,
	comment,
	user,
	bot,
	minor,
	(revision_new - revision_old) AS revision_global_offset,
	(length_new - length_old) AS length_difference,
	uri,
	domain,
	dt,
	patrolled
FROM
	{{ ref('recentchange') }}
WHERE
	"type" = 'edit'