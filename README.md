# Hadoop-Mutual-Friends
<b>Goal: Using hadoop/mapreduce to analyze social network data.</b>

#### Q1 Find “Mutual/Common friend list of two friends".
    For example,
    Alice’s friends are Bob, Sam, Sara, Nancy
    Bob’s friends are Alice, Sam, Clara, Nancy
    Sara’s friends are Alice, Sam, Clara, Nancy

    As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
    As Sara and Bob are not friend and so, their mutual friend list is empty. (exclude them from output). 

    Input:
    Input files 
    1. soc-LiveJournal1Adj.txt 
    The input contains the adjacency list and has multiple lines in the following format:
    <User><TAB><Friends>
    2. userdata.txt
    The userdata.txt contains dummy data which consist of 
    column1 : userid
    column2 : firstname
    column3 : lastname
    column4 : address
    column5: city
    column6 :state
    column7 : zipcode
    column8 :country
    column9 :username
    column10 : date of birth.

    Output: The output should contain one line per user in the following format:
    <User_A>, <User_B><TAB><Mutual/Common Friend List>

#### Q2 Find friend pairs whose number of common friends (number of mutual friend) is within the top-10 in all the pairs (in decreasing order).
    Output Format:
    <User_A>, <User_B><TAB><Mutual/Common Friend Number>

#### Q3 In-memory join
    Question:
    Given any two Users (they are friend) as input, output the list of the names and the states of their mutual friends.

    Output format:
    UserA id, UserB id, list of [states] of their mutual Friends.

    Sample Output:
    26, 28	[Evangeline: Ohio, Charlotte: California]

#### Q4 Using reduce-side join and job chaining:

    Step 1: Calculate the minimum age of the direct friends of each user.
    Step 2: Sort the users by the calculated minimum age from step 1 in descending order.
    Step 3. Output the top 10 users from step 2 with their address and the calculated minimum age.

    Sample output：
    User A, 1000 Anderson blvd, Dallas, TX, minimum age of direct friends.

### How to run
    1. add files into HDFS
    ========
    hadoop fs -mkdir /testFiles
    hadoop fs -copyFromLocal soc-LiveJournal1Adj.txt /testFiles
    hadoop fs -copyFromLocal userdata.txt /testFiles
    =========
    2. run
    =========
    hadoop jar MutualFriend.jar /inputFile/soc-LiveJournal1Adj.txt /outputQ1_5

    hadoop jar Top10.jar /inputFile/soc-LiveJournal1Adj.txt /outputQ2_3 /temp03

    hadoop jar InMemory.jar /inputFile/soc-LiveJournal1Adj.txt /outputQ3_8 /inputFile/userdata.txt 0 1

    hadoop jar ReducerJoin.jar /inputFile/soc-LiveJournal1Adj.txt /inputFile/userdata.txt /tempQ4_17 /outputQ4_28
    =========
    3. copy file to local
    hadoop fs -copyToLocal /outputQ4_6 /usr/local/Cellar/hadoop/result
    





