SELECT rel1.col1, rel2.col1 FROM rel1 JOIN rel2 ON rel1.col1=rel2.col1 WHERE rel1.col2>940;

SELECT * FROM rel1 JOIN rel2 ON rel1.col1 = rel2.col1 WHERE rel1.col2 < 23 OR rel2.col1 > 957; 

SELECT * FROM rel1 JOIN rel2 ON rel1.col1 = rel2.col1 WHERE rel1.col1 < 500 AND rel2.col1>473;

SELECT * FROM rel1 JOIN rel2 ON rel1.col1=rel2.col1 WHERE rel2.col2=rel2.col1;

SELECT * FROM rel3 JOIN rel4 ON rel3.col1 = rel4.col1 WHERE rel3.col2 < 93;
