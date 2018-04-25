-- Create Tables
DROP TABLE IF EXISTS json_table;
DROP TABLE IF EXISTS PronounsList;

CREATE TABLE json_table (json string);
CREATE TABLE PronounsList (pronouns varchar(255));
-- ########################################################################################

/* Load data in Tables given that the data is already at HDFS in below path*/
LOAD DATA INPATH "/user/ubuntu/tweets/files/*.txt" INTO TABLE json_table;
INSERT INTO PronounsList (pronouns) values ("den"),("denna"), ("denne"),("det"),("han"), ("hen"), ("hon");
-- ########################################################################################

-- Query that outputs the count of occurences of the words in the PronounsList in all the tweets datasat available in the HDFS
WITH tokens AS (
  select explode(split(LOWER(json_table_data.text), ' ')) AS word
  from
    (select get_json_object(json_table.json, '$.id') as id, get_json_object(json_table.json, '$.text') as text, get_json_object(json_table.json, '$.retweeted_status') as retweetStatus
    from json_table
    WHERE id is not null AND retweetStatus is null AND text is not null
    ) as json_table_data
)
select word, count(*) as count
from tokens
where word IN (select * from PronounsList)
group by word;
-- ########################################################################################
