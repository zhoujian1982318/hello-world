drop table if exists ex_content_detail;
create EXTERNAL table if not exists ex_content_detail (
	UrAl  struct<ts:BIGINT,
	            aMc:string,
	            ttl:BIGINT,
	            usd:BIGINT,
	            avl:BIGINT,
	            wcStats:array<
	                       struct<wid:INT,
	                              url:string,
	                              clen:BIGINT,
	                              hits:INT,
	                              misses:INT,
	                              bypasses:INT,
	                              errors:INT,
	                              hbytes:BIGINT,
	                              mbytes:BIGINT,
	                              bbytes:BIGINT,
	                              usvr:string,
	                              cdate:BIGINT
	                              >
	            >
	     >
)
ROW FORMAT  serde 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION  'hdfs://wentong-01.eng.hz.relay2.cn:9000/content/detail';
-- with serdeproperties ( 'paths'='requestbegintime, adid, impressionid, referrer, useragent, usercookie, ip' )
-- load data inpath '${hiveconf:inputFile}' into table content_detail;

create table if not exists content_stats (content string, hits INT, clen BIGINT);

INSERT OVERWRITE TABLE content_stats  
select d.content, sum(d.hits) as sumhits, max(d.clen) as maxLen FROM ( select substring_index(b.col.url,'/', -1) as content , b.col.hits as hits, b.col.clen as clen  
			FROM  (select explode(ural.wcStats) as col  from ex_content_detail ) b ) d GROUP BY d.content order by sumhits desc ;


-- elect b.col.url , b.col.hits, b.col.clen  from  (select explode(ural.wcStats) as col  from content_detail ) b;

-- old wrong method

-- SELECT  regexp_replace(regexp_extract(get_json_object(json, '$.UrAl.wcStats') ,'(\\[)(.*?)(\\])', 2),'},{','}|{',) FROM contents;

-- SELECT get_json_object(get_json_object(json, '$.UrAl.wcStats[0]'),'$.url') FROM contents limit 1;


