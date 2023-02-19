create database music_chart;

use music_chart;

create table tracks
(
	id bigint unsigned auto_increment,
	track_id char(32) not null,
	name varchar(100),
	duration int unsigned,
	source char(50) not null,
	release_date datetime not null,
	popularity int unsigned,
	artist_id char(32),
	index (artist_id),
	index (track_id),
	constraint FK_tracks_artist foreign key (artist_id) references artists(artist_id),
	primary key(id)
);


create table track_genres
(
	id bigint unsigned auto_increment,
	track_id char(32) not null,
	genre varchar(50) not null,
	constraint FK_track_genres_track_id foreign key (track_id) references tracks(track_id),
	primary key (id)
);


create table artists
(
	id bigint unsigned auto_increment,
	artist_id char(32),
	name varchar(100),
	source char(50) not null,
	total_followers int unsigned,
	index (artist_id),
	primary key(id)
) 

create table data_time_metadata
(
	id bigint unsigned auto_increment,
	record_id bigint unsigned not null,
	data_time datetime not null,
	record_type char(50) not null,
	index(data_time),
	index(record_id),
	primary key(id)
)

delimiter $$
create function RoundDateTime (input_dt datetime)
returns datetime DETERMINISTIC
begin
	declare temp datetime;
	set temp = input_dt;
	return DATE_FORMAT(DATE_ADD(temp , INTERVAL 30 MINUTE),'%Y-%m-%d %H:00:00');
end;
$$
delimiter ;

delimiter $$
create trigger tracks_metadata
after insert on tracks
for each row
begin
	insert into data_time_metadata
	(record_id, data_time, record_type)
	values (new.id, RoundDateTime(now()), 'tracks');
end;
$$

create trigger artists_metadata
after insert on artists
for each row
begin
	insert into data_time_metadata
	(record_id, data_time, record_type)
	values (new.id, RoundDateTime(now()), 'artists');
end;
$$

create trigger track_genres_metadata
after insert on track_genres
for each row
begin
	insert into data_time_metadata
	(record_id, data_time, record_type)
	values (new.id, RoundDateTime(now()), 'track_genres');
end;
$$
delimiter ;