"""
All databases in SQLebra inherit from SQLebra.database.database.Database, which defines the interface with databases.
This allows SQLebra to work seamlessly with multiple database types (SQLite, MySQL, etc).

Database structure
==================
A database is composed of three tables:

+ VARIABLES: Contains user defined named variables in the database (concept similar to python's "variable"). This is
    the point of access for users. The table has columns:
    - name: varchar(100)
        Unique user defined variable name.
    - id: bigint
        Object identifier associated with the variable.
+ OBJECTS: Contains objects stored in the database (concept similar to python's "object"). This holds the values
    associated to objects. It has columns:
    - id: bigint
        Object's unique identifier
    - type: varchar(100)
        Class name
    - bool_val: tinyint(1)
        1 for True and 0 for False. E.g., used if the stored object is bool.
    - int_val: bigint
        E.g., used if the stored object is int.
    - real_val: double
        E.g., used if the stored object is float.
    - txt_val: text
        E.g., used if the stored object is str.
+ ITEMS: Contains pointers for items of nested objects. This table has columns:
    - id: bigint
        Object identifier
    - key: varchar(500)
        String index (e.g., used by dict)
    - ind: bigint
        Numeric index (e.g., used by list)
    - child_id: bigint
        Object identifier of the object pointed by the item

Example
=======
Let `db` be an open database of any type (MySQL, SQLite, etc). Basic information can be found in:

>> print(db.info)

A database can then be used as a dictionary (see sqlebra.database._dictinterface).

>> db['text'] = 'hello world'
>> print(db['text'])

>> db['x'] = 10
>> print(db['x'] + 2)

>> db['l'] = [0, 1, 2, 3]
>> db['l'].append(4)

All values in a database can be retrieved at once as a dict through the property `py`.

>> print(db.py)

If the database was opened without autocommit, to make the above changes permanent we need to commit them.

>> db.commit()

Once we have finished working with the database, we must close the connection.

>> db.close()
"""