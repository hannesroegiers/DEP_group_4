select kmo.ondernemingsnummer, bedrijfsnaam, ts_rank_cd(ts_tekst, query) as rank, 
	(ts_rank(ts_tekst, query1) + ts_rank(ts_tekst, query2) + ts_rank(ts_tekst, query3)) / 3 as avg,
	ts_rank(ts_tekst, query1) as rank1, 
	ts_rank(ts_tekst, query2) as rank2, 
	ts_rank(ts_tekst, query3) as rank3
	--ts_headline('dutch', tekst, query, 'MaxWords=10,MinWords=1,StartSel = [,StopSel = ],MaxFragments=10,FragmentDelimiter=...')
from jaarverslag jv join kmo on jv.ondernemingsnummer = kmo.ondernemingsnummer, 
	to_tsquery('dutch','(leven & land) | biodiversiteit | (leven & water)') query,
	to_tsquery('dutch','leven & land') query1,
	to_tsquery('dutch','biodiversiteit') query2,
	to_tsquery('dutch','leven & water') query3
where query @@ ts_tekst
order by rank desc
limit 10;

select ondernemingsnummer, ts_rank_cd(ts_tekst, query, 32) as rank, ts_rank_cd(ts_tekst, query) as rank2
from jaarverslag jv, 
	to_tsquery('dutch','(leven & land) | biodiversiteit | (leven & water)') query
where query @@ ts_tekst
order by rank desc
limit 10;

select ondernemingsnummer, ts_rank_cd(ts_tekst, query, 32) as rank
	--ts_headline('dutch', tekst, query, 'MaxWords=10,MinWords=1,StartSel = [,StopSel = ],MaxFragments=10,FragmentDelimiter=...')
from jaarverslag jv, 
	to_tsquery('dutch','(lokale & gemeenschap) | (duurzame & steden & gemeenschappen) | (vrede)') query
where query @@ ts_tekst
order by rank desc
limit 10;