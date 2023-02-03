create database music_chart;
use music_chart;

create table tracks
(
	id char(32),
	name varchar(100),
	duration int unsigned,
	source char(50),
	release_date datetime,
	popularity int unsigned,
	data_time datetime,
	artist char(32),
	index (data_time, artist),
	primary key(id, data_time, artist)
);

create table track_genres
(
	track_id char(32),
	genre varchar(50),
	primary key (track_id, genre)
);

create table artists
(
	id char(32),
	name varchar(100),
	source char(50),
	total_followers int unsigned,
	data_time datetime,
	index (data_time),
	primary key(id)
) 