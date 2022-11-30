alter table jaarverslag
	add column ts_tekst tsvector
	generated always as
	(to_tsvector('dutch', tekst)) stored;

create index tekst_idx on jaarverslag using gin (ts_tekst)
	
-- view available languages	
SELECT cfgname FROM pg_ts_config;