SELECT 
    *
FROM 
    {{ source('bronze', 'recentchange') }}