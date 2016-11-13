# Drastic

This is a shared library for sharing functionality between various components in a running Drastic system.  It is intended to be included by the [drastic-web](https://github.com/UMD-DRASTIC/drastic-web) and [drastic-agent](https://github.com/UMD-DRASTIC/drastic-agent) components.

## Configuration

Configuration is specified with the DRASTIC_CONFIG environment variable

```
export DRASTIC_CONFIG=settings
```

The file at settings.py only needs to be changed should the Cassandra Keyspace name change.  By default the keyspace is called 'drastic'.


## Functionality provided

### Models

#### Activity
#### Blob
#### Collection
#### Group
#### Node
#### Resource
#### Search
#### User

#### Errors

This module contains Drastic model-specific exceptions to be handled if problems
occur when writing to the database, such as UniqueError to be raised when an
attempt is made to write a new Collection with the name of an existing one.


## Command Line Interface

Whenever drastic is installed in a sub-component's virtualenv, the 'drastic-admin' administrative command is available from inside the activated virtualenv to interact with the system. This is separate from the user-level client tool, available at http://github.com/UMD-DRASTIC/drastic-cli.

### Create the database

Creates/Syncs the database defined in configuration with the latest models.  

```
drastic create
```

### Create a user

Provides a guided interface to create a new user. You will be asked if this is an administrative user, their password and their email address.  

```
drastic user-create
```


### List all users

Displays a list of all known users within the current drastic system.

```
drastic user-list
```

### Create a group

Creates a new group within the system. Requires the name of the group and the name of the owning user.

```
drastic group-create GROUP_NAME OWNING_USERNAME
```


### List all groups

List all currently known groups

```
drastic group-list
```


### Add user to group

Adds a user to a group by specifying the group name and the username

```
drastic group-add-user GROUP_NAME USER_NAME
```


### Delete a group

Deletes the specified group

```
drastic group-delete GROUP_NAME
```

### Ingest data

The ```ingest``` command is used to import existing data into the Drastic system.  By providing a directory the command will walk the files and sub-folders within that directory adding them as collections and resources in Drastic.  The created collection structure will mirror the provided local directory.

This command needs at least 3 arguments, they are:

1. **group** - The group name which will be given ownership of the newly created resources and collections
2. **user** - The user who will be used to create the resources and collections.
3. **folder** - The local folder to be ingested.

By default, all created resources are stored in Cassandra (the system default), and are created with URLs that point to a Blob in the Cassandra DB.  It is possible to create the collections and resources but without uploading any files - this will mean that the created resource URLs will point to the local agent (which will then deliver the content).  To perform this type of import the ```noimport ``` and ```localip``` are required.  The first is a boolean flag, the second a string with the IP address of the local agent.

#### Examples

##### Import a local folder into Cassandra

Imports all of the folders and resources under /data into Cassandra.
```
drastic ingest --group TEST_GROUP--user TEST_USER --folder /data
```

##### Import into Drastic but leave the resources on disk

In this case it is important that the agent is configured to point to /data for its disk storage as the URLs created will be relative to this. So a local file /data/test/somedata.csv will be given the URL ```file://192.168.10.10/test/somedata.csv```.
```
drastic ingest --group TEST_GROUP--user TEST_USER --folder /data --localip 192.168.10.10 --noimport
```
