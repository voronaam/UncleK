# Uncle K.

This project is an alternative implementation of a server working on the Apache Kafka protocol. It is very far from being ready, but the development is fairly fast.

## The goal and motivation

The idea of event sourcing behind Apache Kafka is very interesting and has many applications. However, the implementaion side is not impressive. I am not listing Apache Kafka implementations issues here, but they are grave enough to not be fixable by a few pull requests to the main project.

The goal is to provide a scalable alternative to Apache Kafka.

## Current status

I am maintaining a [separate status page](status.md), a changelog and a TODO list in one.

## Running the code

1. Make sure rust build environment is set up
2. Modify [unclek.toml](unclek.toml) to point at a PostgreSQL DB (9.5+)
3. Do `cargo run`

You may want to export `RUST_LOG=debug` for more logging output.

## Frequently Asked Questions

* But are not you simly dumping the data into PostgreSQL now?

A very good ovservation. I am. Being an Engineer I am very serious about establishing the baseline. PostgreSQL performance and scalability are the baseline for the project. There is no reason for any custom data store project to exist if its characteristics are worse than off-the-shelve Postgres. I plan to write my own custom backend, but only after I am happy with the frontend and have done a fair amount of performance tests.

* Do you support all of Apache Kafka features?

No. I have to support most of the operations already (Api Keys in Kafka's therminology) but I am not following any of the Kafka design documents and I am not going to. For example, not supporting consumer group at the moment is a full implementaion of the Consumer Groups API, which happily assigns anybody who talks to it to be its own group coordinator.

* Why Rust?

Rust is a powerful language, but it is very low level and helps me to avoid many a pitfals of Apache Kafka implementation.

* Do you see any practical use case for Uncle K.?

Sure. IoT (Internet of Things) is coming and those things will produce a lot of events. And they are not going to have gigabytes of RAM. At least not right away. But Apache Kafka is huge and recommended RAM is in the range of tens of gigabytes (64Gb, actually). Uncle K. is a 6Mb executable (statically linked release build) which runs in under 100mb of RAM (when under load). Of course the current implementation needs PostgreSQL server as well. You should take that into account.

* Talk is cheap, show me the code

You are very welcome to look at the code. It is not pretty. It is fast and loose. It will get better, I promise. I am not going to BS you saying how my application is scalable until I have a proof that it is.

* Can I use it in production?

Please don't. If you really want to, talk to me first. We can make it work decently for your particular use case.

* Is Apache Kafka Protocol any good? If not, why are you using it?

It... has its issues. I can talk about its flaws for hours. But do not take my word for it, take a tcdump of the vanilla server and see for yourself. But the clients for the protcol have been written in many languages and people are using it. My goal is to write a decent drop-in replacement first. And fix the protocol later.
