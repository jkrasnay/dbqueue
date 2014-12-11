# DbQueue - Database-backed Queue

This is a queue mechanism that is based on a database table and a local thread
pool. 

## Usage

DbQueue is pretty tightly coupled to Spring, particularly so that it can
leverage Spring's `PlatformTransactionManager` abstraction. Use DbQueue as
follows:

- create the `QueueMessage` table as per the given `liquibase.xml` file
- register an instance of `QueueMessageDaoImpl` in your application's Spring 
  context
- for each queue you'd like to implement, subclass `QueueReader` and implement
  its `onMessage(QueueMessage)` method
  
Each `QueueReader` implements a single thread that reads queue messages with a 
given name and calls the `onMessage` method. We don't yet support multi-threaded
readers.
