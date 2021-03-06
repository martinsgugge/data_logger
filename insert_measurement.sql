create or replace function insert_measurement(i_tag_id integer, i_meas_time timestamp without time zone, i_meas_value real)
returns void as
$$
BEGIN
	INSERT INTO measurement
	values(i_tag_id, i_meas_time, i_meas_value);
	
END
$$
	LANGUAGE 'plpgsql';
select * from measurement order by meas_time desc

select insert_measurement(1,	'2021-11-24 23:18:10.399729'::timestamp,	21.6326)
