CREATE TABLE jobs ( url character varying(1024) PRIMARY KEY, priority bigserial );

CREATE TABLE urls ( 
	url character varying(1024) PRIMARY KEY,
	digest character varying(256),
	version smallint
	headers smallint
);
