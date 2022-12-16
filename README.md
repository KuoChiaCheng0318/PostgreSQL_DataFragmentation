# PostgreSQL_DataFragmentation
 Use python to do "range partition", "round robin partition", "round robin insert", "range insert".
 Main python implementation of partition and insert functions are in the Interface.py file.
 The program will load "test_data.txt" file to ratings table.
 <img src="/img/ratingsTable.PNG" alt="Alt text" title="Optional title">
 
 "range partition" will load data from ratings table, then partition the data to specific number of tables base on ratings.
 "round robin insert" will insert data to the range partitioned table base on ratings.
 <img src="/img/rangePartition&Insert.PNG" alt="Alt text" title="Optional title">
 
 "round robin partition" will load data from ratings table, then partition the data to specific number of tables base on the order of data in the original ratings table.
 "range insert" will insert data to the range partitioned table base on the order of data in the original ratings table.
 <img src="/img/roundRobinPartion&insert.PNG" alt="Alt text" title="Optional title">
