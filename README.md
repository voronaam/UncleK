# Uncle K.

This project is an alternative implementation of a server working on the Apache Kafka protocol. It is very far from being ready, but the development is fairly fast.

## The goal and motivation

The idea of event sourcing behind Apache Kafka is very interesting and has many applications. However, the implementaion side is not impressive. It has been written by people far removed from the Software Engineering and is suffering from many problems. I am not listing them here, but they are grave enough to not be fixable by some pull requests to the main project.

The goal is to provide a scalable alternative to Apache Kafka.

## Frequently Asked Questions

* But are not you simly dumping the data into PostgreSQL now?

A very good ovservation. I am. Being an Engineer I am very serious about establishing the baseline. PostgreSQL performance and scalability are the baseline for the project. There is no reason for any custom data store project to exist if its characteristics are worse than off-the-shelve Postgres. I plan to write my own custom backend, but only after I am happy with the frontend and have done a fair amount of performance tests.

* Do you support all of Apache Kafka features?

No. I have to support most of the operations already (Api Keys in Kafka's therminology) but I am not following any of the Kafka design documents and I am not going to. For example, not supporting consumer group at the moment is a full implementaion of the Consumer Groups API, which happily assigns anybody who talks to it to be its own group coordinator.

* Why Rust?

Rust is a powerful language, but it is very low level and helps me to avoid many a pitfals of Apache Kafka implementation.

* Do you see any practical use case for Uncle K.?

Sure. IoT (Internet of Things) is coming and those things will produce a lot of events. And they are not going to have gigbytes of RAM. At least not right away. But Apache Kafka is huge and recommended RAM is in the range of tens of gigabytes (64Gb, actually). Uncle K. is a 5Mb executable (statically linked release build) which runs in under 100Kb of RAM (when under load). Of course the current implementation needs PostgreSQL server as well. But an empty and idle PostgreSQL is well under 1Mb RAM as well.

* Talk is cheap, show me the code

You are very welcome to look at the code. It is not pretty. It is fast and loose. It will get better, I promise. I am not going to BS you saying how my application is scalable until I have a proof that it is.

* Can I use it in production?

Please don't. If you really want to, talk to me first. We can make it work decently for your particular use case.

* Is Apache Kafka Protocol any good? If not, why are you using it?

It is terrible. I can talk about its flaws for hours. But do not take my word for it, take a tcdump of the vanilla server and see for youself. But the clients for the protcol has been written in many languages and people are using it. My goal is write a decent drop-in replacement first. And fix the protocol later.
