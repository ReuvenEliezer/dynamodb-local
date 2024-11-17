# How to run dynamodb server - local

Use docker compose file to run the dynamodb server locally Or Use workbench and enable DDB local in Workbench:
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/workbench.html:
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html

## How to connect to dynamodb server using workbench

![dynamodb-workbench.png](dynamodb-workbench.png)
![dynamodb-workbench-2.png](dynamodb-workbench-2.png)


## User-Messages table: PartitionKey: `userId` and SortKey: `MessageUuid`
![dynamodb-workbench-table.png](dynamodb-workbench-table.png)

## Index table: PartitionKey: `userId` and SortKey: `CreatedTime`
![dynamodb-workbench-index.png](dynamodb-workbench-index.png)