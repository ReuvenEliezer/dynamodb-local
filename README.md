# How to run dynamodb server - local

Use docker compose file to run the dynamodb server locally Or Use workbench and enable DDB local in Workbench:
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/workbench.html:
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html

## How to connect to dynamodb server using workbench

![img_1.png](img_1.png)
![img.png](img.png)

## User-Messages table: PartitionKey: `userId` and SortKey: `MessageUuid`
![img_4.png](img_4.png)

## Index table: PartitionKey: `userId` and SortKey: `CreatedTime`
![img_3.png](img_3.png)