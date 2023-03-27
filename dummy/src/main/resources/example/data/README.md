**the filename and data format of the historical data**

**filename:** [type]\_[machineId]\_[partId].csv

**data format:** attribute1|attribute2|attribute3

```
person_0_0.csv
id|name

company_0_0.csv
id|name

account_0_0.csv
id|createTime|isBlocked|type

loan_0_0.csv
id|loanAmount|balance

medium_0_0.csv
id|type|isBlocked

person_workIn_company_0_0.csv
personId|companyId

person_invest_company_0_0.csv
personId|companyId|timestamp

person_guarantee_person_0_0.csv
personId|personId|timestamp

person_own_account_0_0.csv
personId|accountId

person_apply_loan_0_0.csv
personId|loanId|timestamp

company_guarantee_company_0_0.csv
companyId|companyId|timestamp

company_invest_company_0_0.csv
companyId|companyId|timestamp

company_own_account_0_0.csv
companyId|accountId

company_apply_loan_0_0.csv
companyId|loanId|timestamp

account_transfer_account_0_0.csv
accountId|accountId|timestamp|amount|type

account_withdraw_account_0_0.csv
accountId|accountId|timestamp|amount

account_reply_loan_0_0.csv
accountId|loanId|timestamp|amount

loan_deposit_account_0_0.csv
loadId|accountId|timestamp|amount

medium_signIn_account_0_0.csv
mediumId|accountId|timestamp
```



**the filename and data format of the incremental data (write queries)**

**filename:** write\_[machineId]\_[partId].csv

**data format:** expiryTimeStamp|dependencyTimeStamp|writeId|param1|param2|param3

*All write params are placed in a file for write order.*

```
write_0_0.csv
expiryTimeStamp|dependencyTimeStamp|writeId|params

The sequence of update parameters is as follows:
Write1
personId|personName|accountId|currentTime|accountBlocked|accountType

Write2
companyId|companyName|accountId|currentTime|accountBlocked|accountType

Write3
srcId|dstId|timestamp|amount

Write4
srcId|dstId|timestamp|amount

Write5
personId|currentTime|loanId|loanAmount

Write6
companyId|currentTime|loanId|loanAmount

Write7
accountId|mediumId|mediumBlocked|currentTime

Write8
accountId|loanId|currentTime|amount

Write9
accountId|loanId|currentTime|amount

Write10
accountId

Write11
personId

Write12
mediumId

Write13
pid1|pid2|currentTime

Write14
id

Write15
id


.e.g 
write1
1669111055000|1669111055001|1001|001|Alice|002|2012-11-29T02:53:05.518+0000|false|personalDeposit

```

**the filename and data format of the incremental data (readWrite queries)**

**filename:** readWrite\_[machineId]\_[partId].csv

**data format:** expiryTimeStamp|dependencyTimeStamp|writeId|param1|param2|param3

*All read write params are placed in a file for write order.*

```
readWrite_0_0.csv
expiryTimeStamp|dependencyTimeStamp|writeId|params

The sequence of update parameters is as follows:
ReadWrite1
srcId|dstId|startTime|endTime

ReadWrite2
srcId|dstId|threshold|startTime|endTime

ReadWrite3
srcId|dstId|threshold|startTime|endTime

```


**the filename and data format of the read input**

**filename:** [queryType]\_[queryNumber]_param.csv

**data format:** attribute1|attribute2|attribute3

```
complex_1_param.csv
id|startTime|endTime|truncationLimit|truncationOrder

complex_2_param.csv
id|startTime|endTime|truncationLimit|truncationOrder

complex_3_param.csv
id1|id2|startTime|endTime|truncationLimit|truncationOrder

complex_4_param.csv
id1|id2|startTime|endTime|truncationLimit|truncationOrder

complex_5_param.csv
id|startTime|endTime|truncationLimit|truncationOrder

complex_6_param.csv
id|threshold1|threshold2|startTime|endTime|truncationLimit|truncationOrder

complex_7_param.csv
id|threshold|startTime|endTime|truncationLimit|truncationOrder

complex_8_param.csv
id|ratio|startTime|endTime|truncationLimit|truncationOrder

complex_9_param.csv
threshold|lowerBound|upperBound|startTime|endTime|truncationLimit|truncationOrder

complex_10_param.csv
id1|id2

complex_11_param.csv
id|k

complex_12_param.csv
id|startTime|endTime|truncationLimit|truncationOrder

complex_13_param.csv
id|startTime|endTime|truncationLimit|truncationOrder
```

