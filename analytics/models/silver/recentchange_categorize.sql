SELECT
	id,
	namespace,
	title,
	comment,
	regexp_extract(comment, '(\[\[[^[]*\]\])') as object,
	CASE
		WHEN contains(comment, 'added') THEN 'add'
		WHEN contains(comment, 'remove') THEN 'remove'
		ELSE NULL
	END as action_type_en,
	user,
	bot,
	minor,
	uri,
	domain,
	dt
FROM
	{{ ref('recentchange') }}
WHERE
	"type" = 'categorize'