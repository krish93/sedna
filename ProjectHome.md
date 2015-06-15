# Design Principle & Project Target #
**Sedna** is an open source distributed key-value storage system based on memory to provide a higher throughput and lower latency R/W operation for real time processing in Cloud. Sedna runs on _ZooKeeper_ - an open source coordinator service, and Memcached, an open source key-value storage system.

Sedna is designed to provide highly available, scalable, and easy deployment service for running data centers. Sedna aims to run on top of an infrastructure of thousands of nodes with abundant memory, built for realtime workloads. At this scale, failure happens continuously, Sedna try to manage the data consistency and persistent state in the face of these failures and gives the softwares the reliability and scalability. While in many ways Sedna resembles ideas from other disk based storage system and shares many design and implementation strategies therewith, Sedna does provide some new features which we believe are important for realtime processing of massive data in cloud applications. These features include:
  * 1) it is easy to be depolyed into the current hierarchical storage architecture
  * 2) it supports customize persistent strategy
  * 3) it supports lock-free writes and provides different methods to handle writes conflicts
  * 4) it supports read trigger to accelerate realtime processing
  * 5) it provides MapReduce\cite{mapreduce} interface
  * 6) it provides better scalability and fault tolerance.
Sedna system was designed to run on cheap commodity hardware, handle high write thoughtput, and also provide high data push throughput.

There are still many features under development, so we have not provided all the function list before, please wait with patient or just join us to finish whole project. :)


---


# Introduction #

## Installation ##

New users please read this page first to install Sedna on your system: [WelcomeToSedna](WelcomeToSedna.md)

## Usage ##

After Installation, Users of Sedna can follow these steps: [SednaUsage](SednaUsage.md)