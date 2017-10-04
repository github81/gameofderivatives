


CREATE TABLE swap (id text, organization text, tradedate timestamp, settlementdate timestamp, maturitydate timestamp, notional double, counterparty text, longshort text, payccy text, receiveccy text, payindex text, receiveindex text, frequency text, valuationdate timestamp, swapvalue double, source_stream text, PRIMARY KEY ((organization,id,payccy,receiveccy), valuationdate), ) WITH CLUSTERING ORDER BY (valuationdate DESC);


-- insert statement examples
INSERT INTO swap (id, organization, tradedate, settlementdate, maturitydate, notional, counterparty, longshort, payccy, receiveccy, payindex, receiveindex,frequency, valuationdate, swapvalue) VALUES ('SWAP100', 'org2472', '2008-07-21 21:34:48', '2008-07-23 21:34:48', '2039-07-23 21:34:48', 31000000, 'org9192','SHORT', 'XAG', 'JPY', 'LIBOR', 'LIBOR','6M', '2017-09-13 14:34:48', 100.23);
INSERT INTO swap (id, organization, tradedate, settlementdate, maturitydate, notional, counterparty, longshort, payccy, receiveccy, payindex, receiveindex,frequency, valuationdate, swapvalue) VALUES ('SWAP100', 'org2472', '2008-07-21 21:34:48', '2008-07-23 21:34:48', '2039-07-23 21:34:48', 31000000, 'org9192','SHORT', 'XAG', 'JPY', 'LIBOR', 'LIBOR','6M', '2017-09-13 14:34:48', 100.24);

-- select with group by on organizatoin
select id, organization, swapvalue, max(valuationdate) from swap group by organization, id, payccy, receiveccy;

select organization, id, payccy, receiveccy, swapvalue, max(valuationdate) from swap where organization='org122' group by organization,id,payccy,receiveccy allow filtering;

