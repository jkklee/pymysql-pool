# PyMySQL_Connection_Pool
A simple connection pool based PyMySQL. Compatible with single thread mode and multi threads mode

When we use pymysql with python multi threads, generally will face the questions:  
1. It can't share a connection created by main thread with all sub-threads.
2. If we make every sub-thread create a connection and close it when this sub-thread end, that's obviously waste.

So I implement this python class aimed at create as least connections with MySQL as possible in multi-threads programing. In another words, reuse the established connections as many as possible.

