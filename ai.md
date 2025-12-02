# AI-Assisted Project Report (20 points)


### **Question 1: Exploring the SQL Database and HDFS file size (5 points)**

A. How In Part 1, you were asked to use an AI assistant to find the necessary SQL commands to explore the `CS544` database. Using your own words (that is don't copy/paste AI output), summarize the AI's response. List the commands the AI suggested and what you discovered using them, such as the table names and the relevant columns for the join operation.(2 points)
```
The AI suggested to use the "show tables;" command to discover what were the table names present in the CS544 database.Two tables were found loans and loan_types using this command. And to check the relevant columns "DESCRIBE" command was recommended. Here, the loan id and loan_type_id columns were common in both tables on which we had to take a join.
```
B. List the HDFS command that AI suggested for exploring the file size. (1 point)
``` 
hdfs dfs -du -h /your/hdfs/directory/* | awk '{print $2, $1, "hdfs://boss:9000"$3}' 
```


C. Using your own words (that is don't copy/paste AI output), summarize the AI's response for your question on why the file size might be different on HDFS. List any additional prompts (do not copy/paste the output for the prompts) that you had to use to get the AI assistant to talk about compression related details.
```
The difference in file size was due to this function "fs.open_output_stream", which had a default replication factor of 3, instead of the 2 mentioned in assignment.
```


### **Question 2: Understanding Performance (5 points)**
```
The "create" operation corresponds to the first execution of CalcAvgLoan for a given county code. Since the partitions directory in HDFS is cleared before the performance tests begin, this initial call   
likely involves the overhead of "Reading and filtering the raw data" and "Physically creating and writing the partitioned files to HDFS". The "reuse" operation corresponds to the second execution of CalcAvgLoan for the same county code, immediately after the "create" operation. In this case, the partitioned files for that specific county code already exist in HDFS. In essence, the "create" time includes the one-time cost of data organization and writing to a partitioned structure, while the "reuse" time reflects the more efficient subsequent reads from an already-optimized and partitioned dataset.
```

### **Question 3: Analyzing NameNode Logs (10 points)**

In Part 4, after killing a DataNode, you were required to collect the NameNode logs and use an AI to analyze them. Based on your interaction with the AI, answer the following:

A. How does the NameNode detect a DataNode failure? (2 points)
```
the NameNode detects DataNode failures primarily through a heartbeat mechanism, configured by specific properties.                                                     

Specifically, the logs show the following startup arguments:                                                                                                                                                

 * -D dfs.namenode.stale.datanode.interval=10000: This indicates that a DataNode is considered "stale" if the NameNode does not receive a heartbeat from it within 10,000 milliseconds (10 seconds).        
 * -D dfs.namenode.heartbeat.recheck-interval=30000: This indicates that the NameNode rechecks for missed heartbeats every 30,000 milliseconds (30 seconds).                                                

Therefore, the NameNode detects DataNode failures by monitoring regular heartbeats from DataNodes and marking them as stale or potentially failed if heartbeats are not received within the configured      
intervals.  
```

B. What specific log messages indicate that blocks are now under-replicated?(2 points)
```
Based on the namenode_logs.txt file, the following log messages indicate information about under-replicated blocks
2025-10-27 21:55:44,804 INFO hdfs.StateChange: STATE* UnderReplicatedBlocks has 0 blocks                                                                                                                    
                                                                                                                                                                                                            
And as part of a summary of block states:                                                                                                                                                               
                                                                                                                                                                                                            
2025-10-27 21:55:44,808 INFO blockmanagement.BlockManager: Number of under-replicated blocks = 0
These lines typically appear during the NameNode's startup or after a recheck of block states.
```

C. How long did HDFS take to officially mark the DataNode as dead?(3 points)
```
HDFS marked the DataNode as dead 1 minute and 59.202 seconds after it registered.                                                                                                                           

 • DataNode (172.18.0.5:9866) registered at 21:55:45,603.                                                                                                                                                   
 • DataNode (172.18.0.5:9866) was marked as dead at 21:57:44,805. 
```

D. What recovery mechanisms did HDFS attempt automatically?(3 points)
```
The automatic recovery mechanisms HDFS attempted were:                                                                                                                          

 1 Removing the dead DataNode from its internal state: The NameNode detected a lost heartbeat from 172.18.0.5:9866 and proceeded to remove it and its associated blocks from the block map. 2025-10-27      
   21:57:44,805 INFO hdfs.StateChange: BLOCK* removeDeadDatanode: lost heartbeat from 172.18.0.5:9866, removeBlocksFromBlockMap true                                                                        
 2 Updating the network topology: The DataNode was also removed from the cluster's network topology. 2025-10-27 21:57:44,808 INFO net.NetworkTopology: Removing a node: /default-rack/172.18.0.5:9866    
```
