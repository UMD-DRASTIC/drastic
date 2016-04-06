# Indigo

This is a shared library for sharing functionality between various components in a running Indigo system.  It is intended to be included by the [indigo-web](https://bitbucket.org/archivea/indigo-web) and [indigo-agent](https://bitbucket.org/archivea/indigo-agent) components.

## Configuration

Configuration is specified with the INDIGO_CONFIG environment variable

```
export INDIGO_CONFIG=settings
```

The file at settings.py only needs to be changed should the Cassandra Keyspace name change.  By default the keyspace is called Indigo.


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

This module contains Indigo model-specific exceptions to be handled if problems
occur when writing to the database, such as UniqueError to be raised when an
attempt is made to write a new Collection with the name of an existing one.


## Command Line Interface 

Whenever indigo is installed in a sub-component's virtualenv, the indigo command is available from inside the activated virtualenv to interact with the system.  

### Create the database 

Creates/Syncs the database defined in configuration with the latest models.  

```
indigo create
```

### Create a user 

Provides a guided interface to create a new user. You will be asked if this is an administrative user, their password and their email address.  

```
indigo user-create
```

    
### List all users 

Displays a list of all known users within the current indigo system.

```
indigo user-list
```

### Create a group

Creates a new group within the system. Requires the name of the group and the name of the owning user.

``` 
indigo group-create GROUP_NAME OWNING_USERNAME
```


### List all groups 

List all currently known groups 

```
indigo group-list
```


### Add user to group

Adds a user to a group by specifying the group name and the username

```
indigo group-add-user GROUP_NAME USER_NAME
```


### Delete a group

Deletes the specified group 

```
indigo group-delete GROUP_NAME
```
    
### Ingest data 

The ```ingest``` command is used to import existing data into the Indigo system.  By providing a directory the command will walk the files and sub-folders within that directory adding them as collections and resources in Indigo.  The created collection structure will mirror the provided local directory.

This command needs at least 3 arguments, they are:

1. **group** - The group name which will be given ownership of the newly created resources and collections
2. **user** - The user who will be used to create the resources and collections.
3. **folder** - The local folder to be ingested.

By default, all created resources are stored in Cassandra (the system default), and are created with URLs that point to a Blob in the Cassandra DB.  It is possible to create the collections and resources but without uploading any files - this will mean that the created resource URLs will point to the local agent (which will then deliver the content).  To perform this type of import the ```noimport ``` and ```localip``` are required.  The first is a boolean flag, the second a string with the IP address of the local agent.

#### Examples

##### Import a local folder into Cassandra

Imports all of the folders and resources under /data into Cassandra.
```
indigo ingest --group TEST_GROUP--user TEST_USER --folder /data 
```

##### Import into Indigo but leave the resources on disk

In this case it is important that the agent is configured to point to /data for its disk storage as the URLs created will be relative to this. So a local file /data/test/somedata.csv will be given the URL ```file://192.168.10.10/test/somedata.csv```.
```
indigo ingest --group TEST_GROUP--user TEST_USER --folder /data --localip 192.168.10.10 --noimport
```
