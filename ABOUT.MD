# About the Drastic Repository System
The Drastic repository system is designed for scalability, performance and flexibility in the age of distributed databases and computation. These features
are reflected in the name Drastic, which stands for digital repositories
at scale that invite computation to improve collections. Drastic provides core repository services to support object persistence, curation, and custom processing workflows. It can accommodate any number of files and folders without degrading performance by scaling up horizontally, which means that adding storage servers to the Drastic database cluster creates storage capacity without adding overhead. Drastic is an open source, community project to create
scalable, next-generation digital repositories.

## Interacting with Drastic
Drastic's core features center around the management of folders, files, and key-value properties (or metadata). Users can manage objects in Drastic through a variety of interfaces, including a web interface, command-line tool, and standards-based RESTful application programming interface (API). The API standard is CDMI, the Cloud Data Management Interface from the Storage Network Industry Association (SNIA). All of these interface support create, read, update, and delete functions for folders, files, access controls, and properties. The web and command-line tools also allow for the creation of Drastic users and groups.

## Features at a Glance
* Horizontal scaling to billions of files and beyond
* Key-value properties for metadata
* Group-based access controls
* Web user interface
* Command-line client tool
* Industry standard REST storage API (CDMI)
* Message publishing after any data event over MQTT

## Architecture
Drastic is written in Python and we have chosen an AGPL license to ensure that the source code is always be freely available, including derivative projects. The code can be found on GitHub (https://github.com/UMD-DRASTIC/). Drastic's core persistence is the Apache Cassandra database, a distributed wide column store, originally developed by Facebook and currently in use by many web-scale businesses. Cassandra achieves high throughput at any scale by relaxing the usual consistency requirements of traditional databases. This is a trade-off that works very well for the use cases of repository data, which tend to be written once and often in large batches, with more data added incrementally over time. In addition we are discovering extra value in the Cassandra persistence layer, including parallel computation and the ability to also down-scale a cluster.
