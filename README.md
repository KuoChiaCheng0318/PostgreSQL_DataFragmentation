# PostgreSQL_DataFragmentation
 Developed Python scripts to do "range partition" and "round-robin partition". <br />
 range partition will partition data based on the value of data <br />
 round-robin partition will partition data based on the sequence of data <br />
 Main python implementation of partition and insert functions are in the [Interface.py](Interface.py) file. <br />
 <br />
 The program will first load "test_data.txt" file to ratings table.	<br />
 <img src="/img/ratingsTable.PNG" alt="Alt text" title="Optional title"> <br /> <br />
 
 "range partition" will load data from ratings table, then partition the data to specific number of tables based on the value of ratings. <br />
 "range insert" will insert data to the range partitioned table based on the value of ratings. <br />
 <img src="/img/rangePartition&Insert.PNG" alt="Alt text" title="Optional title"> <br />
  <br />
 "round-robin partition" will load data from ratings table, then partition the data to specific number of tables based on the sequence of data in the original ratings table. <br />
 "round-robin insert" will insert data to the range partitioned table based on the sequence of data in the original ratings table. <br />
 <img src="/img/roundRobinPartion&insert.PNG" alt="Alt text" title="Optional title"> <br />
