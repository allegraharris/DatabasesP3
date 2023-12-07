SELECT rel1.colOne, rel2.colOne FROM rel1 JOIN rel2 ON rel1.colOne=rel2.colOne WHERE rel1.colA>940;

SELECT * FROM rel1 JOIN rel2 ON rel1.colOne = rel2.colOne WHERE rel1.colA < 23 OR rel2.colOne > 957; 

SELECT * FROM rel1 JOIN rel2 ON rel1.colOne = rel2.colOne WHERE rel1.colOne < 500 AND rel2.colOne>473;

SELECT * FROM rel1 JOIN rel2 ON rel1.colOne=rel2.colOne WHERE rel2.colB=rel2.colOne;

SELECT * FROM rel3 JOIN rel4 ON rel3.colOne = rel4.colOne WHERE rel3.colC < 93;
