# Tutorial
Here you have how to start using 'PgOperations':
## Installation
It is good practice always use Python [virtual environments](https://docs.python.org/3/library/venv.html) to install new Python libraries. Navigate to the folder where you want to create the virtual environment, for example the 'venvs' folder, and type:

    python3 python3 -m venv pgOperations
   
Activate the virtual environment to install the modules:
   
    source pgOperations/bin/activate

Use [pip](https://pypi.org/project/pip/) to install the modules in the virtual environment:

    (pgOperations)$pip install psycopg2
    (pgOperations)$pip install pgOperations

Now you are ready to use the library to edit data in PostgreSQL.

## Create a database

To create a database it is necessary first to have an opened connection. Create a regular psycopg2 connection to the 'postgres' database:

    conn=psycopg2.connect(database="postgres", user="postgres", 
                password="postgres", host="localhost", port=5432) 

Create a <a href="../reference/#src.pgOperations.pgOperations.PgConnection" target="_blank">PgConnection</a> instance:

    pgc = pg.PgConnection(conn) 

Create a <a href="../reference/#src.pgOperations.pgOperations.PgDatabases" target="_blank">PgDatabases</a> instance. This class allow to create and delete databases:

    pgdb=pg.PgDatabases(pgConnect=pgc)

Create the database:

    pgc2=pgdb.createDatabase(databaseName="pgoperationstest",
                addPostgisExtension=True,closeNewConnection=False)

The <a href="../reference/#src.pgOperations.pgOperations.PgDatabases.createDatabase" target="_blank">createDatabase</a> 
method returns an opened PgConnect instance, connected to the new database. 

## Create a table
Execute regular SQL code to create an scheme and a table in the `pgoperationstest` database, continuing with the previous listing:

    pgc2.cursor.execute("create schema d")
    pgc2.cursor.execute("create table d.points (gid serial primary key, 
                description varchar, depth double precision, 
                geom geometry('POINT',25831))")

Commit the changes and close the connections to the databases `postgres` and `pgoperationstest`:

    pgc2.commit()
    pgc2.disconnect()
    pgc.disconnect()

## Edit data
The examples in this section use the table `d.points` created in the [create a table](#create-a-table) section. This table has the fields `gid`, `description`, `deph` and `geom` fields. The `geom` field is a PostGIS <a href="http://postgis.net/workshops/postgis-intro/geometries.html/" target="_blank">geomerty</a> field type, of type `POINT` in the SRC `25831`. 

> **_NOTE:_**  It is not necessary to have a geometry field in the tables to use the pgOperations module.

Also, in the next examples it is supposed the `pgOperations` module has been imported, 
stored in the `pg` variable, and there is
a <a href="../reference/#src.pgOperations.pgOperations.PgOperations" target="_blank">PgOperations</a>  instance stored in the variable called ´pgo´:

    from pgOperations import pgOperations as pg
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", 
            password="postgres", host="localhost", port="5432")
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=True))

**This library supposes all python dictionary keys match with the table field names**

### Insert

To insert you can use the method <a href="../reference/#src.pgOperations.pgOperations.PgOperations.pgInsert" target="_blank">PgOperations.pgInsert</a>. In the next sections you will
 see different use cases.

#### Example without PostGIS geometry field

Example with automatic generation of the SQL expression, using a dictionary,
where the key names must be match with the table field name. The order is 
indifferent. In this case there is not geometry field:

    d={"description": "water well", "depth": 12.15}
    fieldsAndValues=pg.FieldsAndValues(d=d)
    resp=pgo.pgInsert(table_name="d.points", fieldsAndValues=fieldsAndValues, 
            str_fields_returning="gid")
    print(resp)

The outputs are the following:

    pgInsert
    Query:  insert into d.points (description,depth) values (%s,%s) returning gid
    Values:  ['water well', 12.15]
    [{'gid': 1}]

Below the prints are explained:

- The first three prints are printed because the sentence `global_print_queries` 
  was set to `True` on creating the object `pgo`. It prints the SQL sentence 
  to execute with a `Psycopg2` cursor:
  `'insert into d.points (description,depth) values (%s,%s) returning gid'`.
  The method also prints the values to be used: `['water well', 12.15]`.

- The last print `[{'gid': 1}]` shows the returned value. The returned value
can be an empty list, or several fields values, 
specified in the parameter `str_fields_returning="gid"`, in a dictionary.

#### Example with a PostGIS geometry field
In this example the coordinates of the geometry are in the SRC EPSG 25830,
but the table requires the SRC EPSG 25831. The example reprojects the coordinates
to be able to be inserted in the table. This is not necessary if the
original coordinates of the geometry match with the SRC of the geometry
field of the table.

    d={"description": "water well", "depth": 12.15, "geom": "POINT(100 200)"}
    geometryFieldOptions=pg.GeometryFieldOptions(geom_field_name="geom", 
            epsg='25830',epsg_to_reproject="25831")
    fieldsAndValues=pg.FieldsAndValues(d=d, list_fields_to_remove=["depth"], 
            geometryFieldOptions=geometryFieldOptions)
    resp=pgo.pgInsert(table_name="d.points", fieldsAndValues=fieldsAndValues, 
            str_fields_returning="gid")
    print(resp)

The outputs are the following:
    pgInsert
    Query:  insert into d.points (description,geom) values (%s,st_transform(st_geometryfromtext(%s,25830),25831)) returning gid
    Values:  ['water well', 'POINT(100 200)']
    [{'gid': 2}]

#### Example generating the expressions manually
This example manually generates the expressions using the class 
[FieldsAndValuesBase][src.pgOperations.pgOperations.FieldsAndValuesBase],
so you are free to write the SQL sentence, for more complicated cases.

    fieldsAndValuesBase=pg.FieldsAndValuesBase(
        str_field_names="depth, description, geom", 
        list_field_values=[12.15, "water well","POINT(100 200)"], 
        str_s_values="%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)")
    
    resp=pgo.pgInsert(table_name="d.points", fieldsAndValues=fieldsAndValuesBase, 
            str_fields_returning="gid")
    print(resp)

Result:

    pgInsert
    Query:  insert into d.points (depth, description, geom) values (%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)) returning gid
    Values:  [12.15, 'water well', 'POINT(100 200)']
    [{'gid': 3}]

### Update

To update a table you can use the method <a href="../reference/#src.pgOperations.pgOperations.PgOperations.pgUpdate" target="_blank">PgOperations.pgUpdate</a>. In the next sections you will
 see different use cases.

#### Example using a Python dictionary

The following example updates al the rows where `gid=1`, and only updates the field `geom` because the
field `description` is elimitated:

    d={"description": "water well2", "geom": "POINT(300 300)"}
    geometryFieldOptions=pg.GeometryFieldOptions(
        geom_field_name="geom",epsg=25831
    )
    fieldsAndValues=pg.FieldsAndValues( d=d, 
                                        list_fields_to_remove=['description'], 
                                        geometryFieldOptions=geometryFieldOptions
                                        )
    whereClause=pg.WhereClause(where_clause="gid=%s", where_values_list=[1])
    resp=pgo.pgUpdate(table_name="d.points", 
                      fieldsAndValues=fieldsAndValues, 
                      whereClause=whereClause)
    print (resp)

The result of the previous listing is:

    pgUpdate
    Query: update d.points set (geom) = row(st_geometryfromtext(%s,25831)) where gid=%s
    where_clause:  gid=%s
    where_values_list [1]
    New field values:  ['POINT(300 300)']
    Number of rows updated:  1
    1
    
The returned value was `1` therefore only one row has been updated.

#### Example generating the SQL expression manually

Next example updates the fields `description` and `geom`, generating the expression 
manually:

    fieldsAndValuesBase=pg.FieldsAndValuesBase(
        str_field_names="description, geom", 
        list_field_values=["water well updated","POINT(300 300)"], 
        str_s_values="%s,st_transform(st_geometryfromtext(%s,25830),25831)")
    whereClause=pg.WhereClause(where_clause="gid=%s", where_values_list=[1])
    resp=pgo.pgUpdate(table_name="d.points", 
                      fieldsAndValues=fieldsAndValuesBase, 
                      whereClause=whereClause
                      )
    print (resp)     

The result of the previous listing is:

    pgUpdate
    Query: update d.points set (description, geom) = row(%s,st_transform(st_geometryfromtext(%s,25830),25831)) where gid=%s
    where_clause:  gid=%s
    where_values_list [1]
    New field values:  ['water well updated', 'POINT(300 300)']
    Number of rows updated:  1
    1
    
One row has been updated.

### Select

To select you can use the method <a href="../reference/#src.pgOperations.pgOperations.PgOperations.pgSelect" target="_blank">PgOperations.pgSelect</a>. In the next listing you will
find an example.

The pgSelect method returns the selected rows as list of dictionaries, or as list of tuples. 
Each element of the list represents a selected row.

Example:

    whereClause=pg.WhereClause(where_clause='gid > %s and gid < %s', where_values_list=[0, 3])
    resp=pgo.pgSelect(table_name="d.points", 
                      string_fields_to_select='gid,depth,description,st_astext(geom)',
                      whereClause=whereClause,
                      print_query=False,
                      orderBy='gid desc'
                      )
    print(resp)

The result of the previous command is:

    pgSelect 
    Query:  SELECT array_to_json(array_agg(registros)) FROM 
        (select gid,depth,description,st_astext(geom) from d.points  where gid > %s and gid < %s  order by gid desc limit 100) as registros
    where_clause:  gid > %s and gid < %s
    where_values_list [0, 3]
    Num of selected rows:  2
    [
        {'gid': 2, 'depth': None, 'description': 'water well', 'st_astext': 'POINT(-673652.1897909392 202.79364615897794)'}, 
        {'gid': 1, 'depth': 12.15, 'description': 'water well updated', 'st_astext': 'POINT(-673449.3960596526 304.1894476513122)'}
    ]
    Num of selected rows:  2
    
In the above example, if you set the parameter `get_rows_as_dicts` to false, 
the result is a list of tuples:

    [
        (2, None, 'water well', 'POINT(-673652.1897909392 202.79364615897794)'), 
        (1, None, 'water well updated', 'POINT(-673449.3960596526 304.1894476513122)')
    ]

### Delete
To delete you can use the method <a href="../reference/#src.pgOperations.pgOperations.PgOperations.pgDelete" target="_blank">PgOperations.pgDelete</a>. In the next listing you will
find an example.

    whereClause=pg.WhereClause(where_clause='gid < %s', where_values_list=[3])
    resp=pgo.pgDelete(table_name="d.points",whereClause=whereClause,print_query=False)
    print(resp)

The result of the above example is:

    pgDelete
    Query:  delete from d.points where gid < %s
    where_clause:  gid < %s
    where_values_list [3]
    Number of rows deleted:  2
    2

## Other utilities

### Delete rows and files
This utility deletes the selected rows, and their associated files in the hard disk.
The rows have to have a field with the file names to delete. The file names
can contain an absolute path, or a relative path. It is possible to complete
relative paths with the parameter `base_path`.

see the method <a href="reference/#src.pgOperations.pgOperations.PgOperations.pgDeleteWithFiles" target="_blank"> PgOperations.pgDeleteWithFiles</a> for mor details about the parameters.

**This method only has been tested in Linux systems**

As an example a new table is needed. To create the new table you can use 
the [PgOperations.createTable][src.pgOperations.pgOperations.PgOperations.pgCreateTable] utility.

    r=pgo.pgCreateTable(table_name_with_schema="d.customers",
            fields_definition="gid serial, name varchar, img varchar",
            delete_table_if_exists= True,print_query=False)  

Now let insert some data:

    d1={"name": "customer 1", "img": "image1.jpg"}
    d2={"name": "customer 2", "img": "image2.jpg"}
    d3={"name": "customer 3", "img": "image3.jpg"}
    l=[d1,d2,d3]
    for d in l:
        fieldsAndValues=pg.FieldsAndValues(d=d)
        r=pgo.pgInsert(table_name="d.customers",
            fieldsAndValues=fieldsAndValues,
            str_fields_returning="gid")
        print("Customer inserted. gid: ", r[0]["gid"])

Suppose the images are im the folder `/home/user/app/media/customers/img`. 
To delete all rows and all images:

    r=pgo.pgDeleteWithFiles(table_name="d.customers",field_name_with_file_name="img",
        base_path="/home/user/app/media/customers/img")

In the previous command as the `whereClause` is None, all the rows are selected.

If in the folder `/home/user/app/media/customers/img` there are 
only the files `image1.jpg` and `image2.jpg`, the result will be:

    {'numOfRowsDeleted': 3, 'deletedFileNames': ["image1.jpg", "image2.jpg"], 
    'notDeletedFilenames': ['image3.jpg'], 
    'base_path': '/home/user/app/media/customers/img'}







