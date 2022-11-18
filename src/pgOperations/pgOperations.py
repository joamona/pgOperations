# -*- coding: utf-8 -*-
'''
#pgOperations module

A simple and light weight module to perform operations in PostgreSQL and PostGIS

author: Gaspar Mora-Navarro

Universitat Politècnica de València

Department of Cartographic Engineering Geodesy and Photogrammetry

Higher Technical School of Geodetic, Cartographic and Topographical Engineering

joamona@cgf.upv.es

This module facilitates to perform the most common operations: 
insert, delete, update, select, create and delete databases.

The class methods receive Python dictionaries, create the SQL sentences and perform the operations.
The methods are able to work also with PostGIS geometries, using WKT representations.

This library depends of the psycopg2 library (http://initd.org/psycopg/).

This library uses Python >= 3
'''
from email.mime import base
from tkinter import NO
from typing import Union
from xmlrpc.client import Boolean, boolean

import psycopg2, os
import psycopg2.extensions
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 

import json

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

class PgConnection():
    """Class to store a Psycopg2 connection and cursor, which are the objects
    used to perform the transactions in the database.
    
    This class do not do anything interesting, but an instance of this class, or an instance of 
    PgConnection, or PgConnect, is necessary to initialize the class PgOperations, which have the 
    real utilities of this library.
        
    This class is the base class for the class [PgConnect][src.pgOperations.pgOperations.PgConnect], 
    which allows creating the Psycopg2 connection with the user credentials.
    """
    conn=None
    """psycopg2 connection object - class variable"""
    cursor=None
    """psycopg2 cursor object - class variable"""
    def __init__(self, psycopg2Connection):
        """
        Constructor

        Examples:
            >>> import psycopg2
            >>> from pgOperations import pgOperations as pg
            >>> conn=psycopg2.connect(database="pgoperationstest", user="postgres", 
                    password="postgres", host="localhost", port=5432)
            >>> pgConnection = pg.PgConnection(conn)
            >>> pgOperations = pg.PgOperations(pgConnection)
        
        args:
            psycopg2Connection (psycopg2 connection): a psycopg2 connection
        """
        self.conn = psycopg2Connection
        self.cursor=self.conn.cursor()

    def disconnect(self):
        """Closes the cursor and the connection. 
        
        It is important to close the connections because PostgreSQL has a limited number
        of connections alive limited. In PostgreSQL, this number is configurable and also, there is a garbage
        collector to close unused connections, but the recommendation is to close the connection once 
        you have finished.
        
        The recommendation is use the connection to perform all the transactions you need, 
        commit the changes and close the Psycopg2 cursor and the connection.
        """
        self.cursor.close()
        self.conn.close()

    def commit(self):
        """Commits the changes in the database. The changes will not be performed until
        you have committed the transaction. You will not see the changes until you commit.
        """
        self.conn.commit()

class PgConnect(PgConnection):
    """Class to create and store a Psycopg2 connection and cursor, which are the objects
    used to perform the transactions in the database.
    
    An instance of this class, or and instance
    of the class [PgConnection][src.pgOperations.pgOperations.PgConnection], 
    is necessary to initialize the class 
    [PgOperations][src.pgOperations.pgOperations.PgOperations], which have the 
    real utilities of this library.
    """
    database:str=None
    user:str=None
    password:str=None
    host:str=None
    port:Union[str,int]=None
    
    def __init__(self, database: str,user: str,password: str,host: str ,port: Union[str,int]):
        """Constructor

        Examples:
            >>> from pgOperations import pgOperations as pg
            >>> pgc=pg.PgConnect(database='pgoperationstest', user="postgres", password="postgres", host="localhost",port=5432)
            >>> pgo=pg.PgOperations(pgConnection=pgc)
            >>> pgc.disconnect()

        Args:
            database: The database name to connect.
            user: The user to connect.
            password: The user password.
            host: Host address.
            port: The port number where PostgreSQL is listening to.
        """
        self.conn=psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        PgConnection.__init__(self,self.conn)
        self.user=user
        self.password=password
        self.host=host
        self.port=port
    
class PgDatabases():
    """
    Class to create and delete databases. To use this class it is necessary to have
    an instance of the class PgConnect
    """
    pgConnect: PgConnect = None

    def __init__(self, pgConnect: PgConnect):
        """Constructor
    
        Examples:
        
            >>> pgc=pg.PgConnect(database='postgres', user="postgres", password="postgres", 
                        host="localhost",port=5432)
            >>> pgdb=pg.PgDatabases(pgConnect=pgc)
            >>> pgc2=pgdb.createDatabase(databaseName="pgoperationstest",
                        addPostgisExtension=True,closeNewConection=False)
            >>> pgc2.cursor.execute("create schema d")
            >>> pgc2.cursor.execute("create table d.points (gid serial primary key, description varchar, depth double precision, geom geometry('POINT',25831))")
            >>> pgc2.commit()
            >>> pgc2.disconnect()
            >>> pgc.disconnect()

        Args:
            pgConnect: PgConnect instance.
        """
        self.pgConnect=pgConnect

    def createDatabase(self, databaseName: str, addPostgisExtension: bool = True, 
            closeNewConnection:bool = False) -> PgConnect:
        """Create a database.

        Examples:

            >>> pgc2=pgdb.createDatabase(databaseName="pgoperationstest",
                    addPostgisExtension=True,closeNewConection=False)
        Args:
            databaseName: The database name.
            addPostgisExtension: Whether or not add the PostGIS extension to the new database. Default True.
            closeNewConnection: Whether or not to close the connection to the new database. Default True.

        Returns:
            A PgConnect instance to manage the newly created database.
        """
        self.pgConnect.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.pgConnect.cursor.execute('create database ' + databaseName)
        pgConnect: PgConnect=PgConnect(database=databaseName, user=self.pgConnect.user, 
            password=self.pgConnect.password, host= self.pgConnect.host, 
            port=self.pgConnect.port)
        if addPostgisExtension:
            pgConnect.cursor.execute("create extension postgis")
            pgConnect.conn.commit()
        if closeNewConnection:
            pgConnect.disconnect()
        return pgConnect

    def dropDatabase(self,databaseName: str):
        """
        Deletes a database.

        Examples:
        
            >>> pgdb.dropDatabase("pgoperationstest")
        """
        self.pgConnect.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.pgConnect.cursor.execute('drop database ' + databaseName)

class FieldsAndValuesBase():
    """Class to store the information that
    [pgInsert][src.pgOperations.pgOperations.PgOperations.pgInsert]
    and [pgUpdate][src.pgOperations.pgOperations.PgOperations.pgUpdate] need
    to work.

    Is the base class for the class [FieldsAndValues][src.pgOperations.pgOperations.FieldsAndValues]
    
    """
    str_field_names: str = None
    list_field_values: str = None
    str_s_values: str = None    
    def __init__(self,str_field_names: str,list_field_values: str, str_s_values: str):
        """Constructor

        Examples:
            >>>fieldsAndValuesBase=FieldsAndValuesBase(
            >>>     str_field_names="depth, description, geom", 
            >>>     list_field_values=[12.15, "water well","POINT(100 200)"], 
            >>>     str_s_values="%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)"
            >>>)

        Args:
            str_field_names: String with the name of the fields of a table.
            list_field_values: List with the values of the fields. 
                                Exactly the same number and in the same order.
            str_s_values: String with a %s for each field value, to use in the 
                                execute method of a psycopg2 cursor.
        """
        self.str_field_names=str_field_names
        self.list_field_values=list_field_values
        self.str_s_values=str_s_values

class GeometryFieldOptions():
    """Class with the details of the geometry field in the dictionary `d`, argument of the
    methods [pgInsert][src.pgOperations.pgOperations.PgOperations.pgInsert] and 
    [pgUpdate][src.pgOperations.pgOperations.PgOperations.pgUpdate]. 
    
    With this class you can set the geometry field name, the current SRC of the geometry, 
    and the new SRC, in case you want ro reproject it. The SRC must be a
    <a href="https://epsg.io/" target="_blank">EPSG</a> code.
    """
    geom_field_name=None, 
    epsg=None
    epsg_to_reproject=None
    def __init__(self,geom_field_name: str,epsg: Union[str, int],
         epsg_to_reproject: Union[str, int]=None):
        """Constructor

        Examples:

            >>>geometryFieldOptions=pg.GeometryFieldOptions(geom_field_name="geom", 
                    epsg='25830',epsg_to_reproject="25831")
        """
        self.geom_field_name=geom_field_name
        self.epsg=str(epsg)
        if epsg_to_reproject is not None:
            self.epsg_to_reproject=str(epsg_to_reproject)


class FieldsAndValues(FieldsAndValuesBase):
    """Create a SQL expression from a Python dictionary. 

    This class is used in the methods 
    [pgInsert][src.pgOperations.pgOperations.PgOperations.pgInsert] and 
    [pgUpdate][src.pgOperations.pgOperations.PgOperations.pgUpdate] of the class 
    [PgOperations][src.pgOperations.pgOperations.PgOperations].
    
    """
    d:dict = None
    list_fields_to_remove: list=None

    def __init__(self, d:dict, list_fields_to_remove: list=None, 
        geometryFieldOptions: GeometryFieldOptions =None):
        """Constructor               
        
        Args:
            d: Dictionary key-value, where the keys are the name fields and the values the value fields of a table,
                e.g. {"depth":12.15, "description":"water well", "geom":"LINESTRING(100 200, 200 200)"}.
                It is not necessary to have a geometry field in the dictionary.
                If there is a geometry field, the value must be in 
                <a href="https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry" 
                    target="_blank">WKT</a> format.
            list_fields_to_remove: list with the dictionary keys to exclude of the SQL expression.
                The corresponding field for the row in the table will not be affected by the insertion,
                or update.
                For example ['gid'] will remove the gid from the expressions and 
                list of values. The value for the field in an insert will be null. The value
                for the field in an update will not be changed.
                Set to None if you do not want to remove any field.
                If the field to remove is not in the dictionary `d` any error will be raised.
            geometryFieldOptions: If there is a geometry field in the table,
                    this object gives the details of the geometry: the field name, the SRC,
                    and the new SRC, in case of a reprojections be required.
        """
        self.__dict_to_string_fields_and_vector_values(
            d, list_fields_to_remove, geometryFieldOptions)

    def __dict_to_string_fields_and_vector_values(
            self,
            d: dict,
            list_fields_to_remove: list, 
            geometryFieldOptions: GeometryFieldOptions):

        if geometryFieldOptions is not None:
            geom_field_name=geometryFieldOptions.geom_field_name
            epsg=geometryFieldOptions.epsg
            epsg_to_reproject=geometryFieldOptions.epsg_to_reproject

        #remove the fields to delete
        if list_fields_to_remove != None:
            for i in range(len(list_fields_to_remove)):
                key=list_fields_to_remove[i]
                if key in d:
                    del d[key]
        
        #forms the tree values returned in the dictionary
        it=list(d.items())
        str_name_fields=""
        list_values =[]     
        str_s_values=""
        for i in range(len(it)):
            str_name_fields = str_name_fields + it[i][0] + ","
            #change the '' values by None
            if it[i][1]=="":
                list_values.append(None) 
            else:
                list_values.append(it[i][1])  
            
            if geometryFieldOptions is None:
                str_s_values=str_s_values + "%s,"
            else:
                if it[i][0] != geom_field_name:
                    str_s_values=str_s_values + "%s,"
                else:
                    if epsg_to_reproject is None:
                        st="st_geometryfromtext(%s,{epsg}),".format(epsg=epsg)
                        str_s_values=str_s_values + st
                    else:
                        st="st_transform(st_geometryfromtext(%s,{epsg}),{epsg_to_reproject}),".format(epsg=epsg, epsg_to_reproject=epsg_to_reproject)
                        str_s_values=str_s_values + st             
                    #(%s,st_geometryfromtext(%s,25830))    
                   
        str_name_fields=str_name_fields[:-1]
        str_s_values=str_s_values[:-1]

        #Initialize  the base class properties
        FieldsAndValuesBase.__init__(self,str_name_fields, list_values, str_s_values)

class WhereClause():
    """
    Class to store the where clause data of the SQL query.
    The where clause must not be written with values, e.g. 
    `'depth > 25.4 and owner_name = "Juan Andrés"'` <-- **do not do that**.

    Instead, the where clause must be divided in two parts:

    * A string expression with this format: `'depth > %s and owner_name = %s'`. 
            This expression is stored in the `where_clause` property.
    * The values to replace the `%s` by real value, e.g. `[25.4,"Juan Andrés"]`.

    The `execute` method of the `cursor` object of the `Psycopg2` library will 
    put the values into the where string expression, in the correct way. You do 
    not have to worry about quotes in the values. See the section <i>Passing parameters 
    to SQL queries</i> in the <a href='https://www.psycopg.org/docs/usage.html' 
    target='_blank'>Psycopg2</a> documentation.

    """
    where_clause:str=None
    where_values_list: list = None
    def __init__(self, where_clause:str,where_values_list: list) -> None:
        """Constructor.

        Args:
            where_clause: The where condition expression, to select the rows to 
                update, or delete. Eg: `'description = %s and depth = %s'`.
                The `where` word must not be in the expression. The values for the where
                condition must not be specified in the string. Instead %s must be used.
                The values of the where condition must be specified in the property 
                `where_values_list`. **Do not quote the %s in the where string**.
            where_values_list: List of the values of the %s in the 
                property `where_clause`, in the order of the %s. e.g. if the 
                where condition is e.g. `'where description = %s and depth = %s'`
                the list of values must have two values, first one the value
                for the field `condition` and the second one the value for
                the field `depth`, e.g. `["This is the description", 25.3]`.
        """
        self.where_clause = where_clause
        self.where_values_list=where_values_list
    def printProperties(self):
        print('where_clause: ', self.where_clause)
        print('where_values_list', self.where_values_list)

class PgOperations():
    """
    Perform the most common operations with PostGIS: insert, delete, update and select. 
    To initialise this class it is necessary a  
    [PgConnection][src.pgOperations.pgOperations.PgConnection], 
    or a [PgConnect][src.pgOperations.pgOperations.PgConnect] instance. 

    All the next examples use the variable pgc (a PgConnect instance), the database
    and the table d.points created in the The 
    <a href="../tutorials/#create-a-table" target="_blank">Create table</a> 
    example. The d.points table has the fields `gid`, `description`, `deph` and `geom`.
    """
    
    query: str = None
    global_print_queries: bool = None
    pgConnection : Union[PgConnect, PgConnection]=None
    autoCommit: bool = None
 
    def __init__(self, pgConnection: Union[PgConnect, PgConnection], 
                autoCommit: bool = True, global_print_queries: Boolean=False):
        """Constructor

        Examples:

            >>> pgo=PgOperations(pgConnection=pgc, autoCommit=True, global_print_queries=True)

        Args:
            pgConnection: PgConnection or PgConnect instance.
            autoCommit: Whether or not perform a commit after each class 
                method call. Default True.
            global_print_queries: For debugging purposes. If True 
                all the methods will print the 
                SQL sentences. All the methods of this class have the parameter `print_query`,
                if any of both variables, `global_print_queries` or `print_query`,
                is True, the SQL queries and data will be printed. 
        """
        self.pgConnection=pgConnection
        self.autoCommit=autoCommit
        self.global_print_queries=global_print_queries
               
    def pgInsert(self, table_name: str, 
            fieldsAndValues: Union[FieldsAndValuesBase,FieldsAndValues], 
            str_fields_returning: str = None, print_query: boolean=False)->list:
        """Inserts a row in a table.

        See examples of use in  <a href="../tutorials/#insert" target="_blank">Examples of insert</a>.

        Args:
            table_name: Table name to insert the row.
            fieldsAndValues: Object containing SQL expression pars of the SQL, and the
                list of values.
            str_fields_returning: A string with the field values, comma separated, of the
                inserted row to be returned, e.g. ´'gid,date'´. The values are put in a 
                dictionary, and the dictionary is put in a list, e.g. `[{'gid': 25, 'date': '2022-11-13'}]`.
                If this parameter is not set None is returned.
            print_query: If `True` the SQL sentence is printed in the screen. This option
                may be is interesting for debugging.
                
        Returns:
            An empty list `[]` or a list with a dictionary, e.g. `[{'gid': 25, 'date': '2022-11-13'}]`, depending of the value of the parameter ´str_fields_returning´.
        """

        conn=self.pgConnection.conn
        cursor=self.pgConnection.cursor
        
        str_field_names=fieldsAndValues.str_field_names
        list_field_values=fieldsAndValues.list_field_values
        str_s_values=fieldsAndValues.str_s_values

        #cons_ins='insert into {0} ({1}) values (%s,st_geometryfromtext(%s,25830))'.format(table_name, string_fields_to_set)
        cons_ins='insert into {0} ({1}) values ({2})'.format(table_name, str_field_names, str_s_values)
        
        if str_fields_returning != None:
            cons_ins =cons_ins + ' returning ' + str_fields_returning
        
        if self.global_print_queries or print_query:
            print('pgInsert')
            print('Query: ', cons_ins)
            print('Values: ', list_field_values)
        self.query=cons_ins
        cursor.execute(cons_ins,list_field_values)

        if self.autoCommit:
            conn.commit()

        if str_fields_returning != None:
            returning=cursor.fetchall()
            fieldNames=str_fields_returning.split(",")
            d={}
            i=0
            for field in fieldNames:
                d[field.strip()]=returning[0][i]
                i=i+1
            return [d]
        else:
            return []
    
    def pgUpdate(self, table_name: str, fieldsAndValues: FieldsAndValues, 
                whereClause: WhereClause=None, 
                print_query: boolean=False) -> int:
        """
        Updates a table
        
        See examples of use in  <a href="../tutorials/#update" target="_blank">Examples of update</a>.

        Args:

            table_name: table name included the schema. Ej. "d.linde". 
                Mandatory specify the schema name, even if the table
                is in the schema `public`: "public.tablename".
            fieldsAndValues: Object containing SQL expression pars of the SQL, and the
                list of values.
            whereClause: Data of the where clause. If it is none all the rows will be updated.
            print_query: print_query: If `True` the SQL sentence is printed in the screen. This option
                may be is interesting for debugging.

        Returns:
            The number of updated rows.
        """
        conn=self.pgConnection.conn
        cursor=self.pgConnection.cursor

        str_field_names=fieldsAndValues.str_field_names
        list_field_values=fieldsAndValues.list_field_values
        str_s_values=fieldsAndValues.str_s_values
        
        cons='update {table_name} set ({str_field_names}) = row({str_s_values})'.format(
            table_name=table_name,str_field_names=str_field_names,str_s_values=str_s_values)
        if whereClause != None:
            cons += ' where ' + whereClause.where_clause
            cursor.execute(cons,list_field_values + whereClause.where_values_list)
        if self.global_print_queries or print_query:
            print('pgUpdate')
            print ('Query: ' + cons)
            if whereClause != None:
                whereClause.printProperties()
            print('New field values: ', list_field_values)
            print('Number of rows updated: ', cursor.rowcount)
        if self.autoCommit:
            conn.commit()
        
        self.query=cons
        return cursor.rowcount
    
    def pgDelete(self, table_name: str, whereClause: WhereClause = None, print_query: boolean=False) -> int:
        """
        Delete rows from a table. Example of use:
        
        See an example of use in  
        <a href="../tutorials/#delete" target="_blank">Example of delete</a>.

        Args:

            table_name: table name included the schema, e.g. `d.linde`. 
                It is mandatory to specify the schema name: `public.tablename`
            whereClause: Data of the where clause. If it is None, all the rows
                will be deleted.
            print_query: If True, will print the SQL query, form debugging purposes.

        Returns:
            The number of deleted rows.
        """
        conn=self.pgConnection.conn
        cursor=self.pgConnection.cursor
        cons='delete from {table_name}'.format(table_name=table_name)
        if whereClause != None:
            cons += ' where ' + whereClause.where_clause
            cursor.execute(cons, whereClause.where_values_list)
        else:
            cursor.execute(cons)
        conn.commit()
        self.query=cons
        if self.global_print_queries or print_query:
            print('pgDelete')
            print('Query: ', cons)
            if whereClause != None:
                whereClause.printProperties()
            print('Number of rows deleted: ', cursor.rowcount)
        
        return cursor.rowcount

    def pgDeleteWithFiles(self, table_name: str, field_name_with_file_name: str,
            whereClause: WhereClause=None, base_path: str=None, 
            print_query: boolean=False) -> dict:
        """
        **This method only has been tested in Linux systems**

        Deletes the selected rows, and their associated files in the hard disk.
        The rows have to have a field with the file names to delete. The file names
        can contain an absolute path, or a relative path. It is possible to complete
        relative paths with the parameter `base_path`.

        Before the rows be deleted, the files are deleted.

        See an example of use in 
        <a href="../tutorials/#delete-rows-and-files">Delete rows and files</a>.
        
        Args:

            table_name: Table name with schema, e.g. `public.customers`. 
            field_name_with_file_name: field in the table which contains the file name, e.g. `img`.
            whereClause: Data of the where clause. If it is None, all the rows and files
                            will be deleted.
            base_path: Base path to prepose to the file names. Can end with or without `/`, e.g. 
                `/home/user/app/media/customers/img`.
            print_query: for debugging purposes. If true will print the SQL sentences and values.

        Returns:

            Dictionary with the following information: 
                number of rows deleted, list of file names deleted, 
                list of filenames not deleted, the base path, e.g. 
                {'numOfRowsDeleted': 3, 'deletedFileNames': [], 
                'notDeletedFilenames': ['image1.jpg', 'image2.jpg', 'image3.jpg'], 
                'base_path': '/home/joamona/temp/img'}
        """
        r=self.pgSelect(table_name=table_name, 
            string_fields_to_select=field_name_with_file_name, 
            whereClause= whereClause, print_query=print_query)
        deletedFileNames=[]
        notExistingFilenames=[]
        for row in r:
            deleted=self.pgDeleteFileInRow(row, field_name_with_file_name,base_path)
            if deleted:
                deletedFileNames.append(row[field_name_with_file_name])
            else:
                notExistingFilenames.append(row[field_name_with_file_name])

        n=self.pgDelete(table_name, whereClause=whereClause)
        return {'numOfRowsDeleted': n, 'deletedFileNames': deletedFileNames, 
            'notDeletedFilenames': notExistingFilenames, 'base_path': base_path}
    
    def pgDeleteFileInRow(self,row: dict, field_name_with_file_name:str, base_path: str=None) -> boolean:
        """
        If you have a row stored in a dictionary, this function
        deletes a file in the file system. The the filename must be one of the values
        of the dictionary. **This method has been texted only in Linux systems**.

        Examples:

            >>>d={'gid': 25, 'description': 'customer', 'img': '00012325.jpg'}
            >>>resp=pgo.deleteFileInRow(row=d,field_name_with_file_name='img',
                base_path='/home/user/media/customers/img')

        Args:
        
            row: A Python dictionary with the filename to delete, 
                e.g. {'gid': 25, 'description': 'customer', 'img': '00012325.jpg'}
            field_name_with_file_name: field in the table which contains the file name. 
            base_path: If the `field_name_with_file_name` field contains relative paths,
                    or only the file name, you can specify in this parameter the
                    base path to complete the absolute path to the file. Does not
                    matters if the base_path ends in the character `/` or not, 
                    e.g. `/home/user/media/images/`, or  `/home/user/media/images`.
        
        Returns: 
            True if the file could be deleted. Otherwise False.
        """
        
        if base_path is not None:
            #print(base_path[len(base_path)-1])
            if base_path[len(base_path)-1]== '/':
                base_path = base_path
            else:
                base_path = base_path + '/'

        imageName=row[field_name_with_file_name]
        if base_path is not None:
            imageName = base_path + imageName 

        if os.path.isfile(imageName):
            os.remove(imageName)
            return True
        return False
            
    def pgSelect(self, table_name: str, string_fields_to_select: str = "*", 
                whereClause: WhereClause = None, 
                get_rows_as_dicts: boolean=True,
                limit: int=100, orderBy: str=None, groupBy: str=None, 
                print_query: boolean=False)->list:
        """
        Select rows of a table.

        See examples of use in  <a href="../tutorials/#select" target="_blank">Examples of select</a>.
        
        Args:
            table_name: table name included the schema, e.g. "d.linde". 
                Mandatory specify the schema name, even if the table is
                in the schema `public`: "public.tablename".
            string_fields_to_select: string with the fields to select, comma separated, 
                e.g. 'gid, description, area, st_asgeojson(geom)'. You can use '*' 
                to select all table fields.
            whereClause: Data of the where clause. If it is none all the rows will be selected.
            get_rows_as_dicts: If True the function will return a list of dictionaries,
                each dictionary representing a selected row. If False the function
                will return a list of tuples. Each list element represents a selected row.
            limit: The maximum rows to return.
            orderBy: Order by SQL clause, e.g. "depth desc". Will order the results
                by the field depth descending. **The words `order by` must not be specified**.
            groupBy: Group by SQL clause, e.g. "depth". Will group the results
                by the field depth. **The words `group by` must not be specified**.
            print_query: print_query: If `True` the SQL sentence is printed in the screen. This option
                may be is interesting for debugging.

        Returns:

            An empty list, if there is not any row selected.
            A list of dictionaries, each dictionary 
                representing a selected row, if `get_rows_as_dicts` is True. 
            A list of lists, each list 
                representing a selected row, if `get_rows_as_dicts` is True.
        """
        cursor=self.pgConnection.cursor

        if orderBy== None:
            orderBy=""
        else:
            orderBy = "order by " + orderBy
            
        if groupBy== None:
            groupBy=""
        else:
            groupBy = "group by " + groupBy
        
        #executes the string. The list_val_cond_where has the values of the %s in the select string by order
        if whereClause == None:
            if get_rows_as_dicts:
                cons='SELECT array_to_json(array_agg(registros)) FROM (select {string_fields_to_select} from {table_name} {groupBy} {orderBy} limit {limit}) as registros'.format(
                    string_fields_to_select=string_fields_to_select,
                    table_name=table_name,limit=limit, orderBy=orderBy, groupBy=groupBy)          
            else:
                cons = 'select {string_fields_to_select} from {table_name} {groupBy} {orderBy} limit {limit}'.format(
                    string_fields_to_select=string_fields_to_select,
                    table_name=table_name,limit=limit, orderBy=orderBy, groupBy=groupBy)
            self.query=cons
            cursor.execute(cons)
        else:
            if get_rows_as_dicts:
                cons='SELECT array_to_json(array_agg(registros)) FROM (select {string_fields_to_select} from {table_name} {cond_where} {groupBy} {orderBy} limit {limit}) as registros'.format(
                    string_fields_to_select=string_fields_to_select,table_name=table_name,
                    cond_where= " where " + whereClause.where_clause, limit=limit, 
                    orderBy=orderBy, groupBy=groupBy)
            else:
                cons='select {string_fields_to_select} from {table_name} {cond_where} {groupBy} {orderBy} limit {limit}'.format(
                    string_fields_to_select=string_fields_to_select,table_name=table_name,
                    cond_where= " where " + whereClause.where_clause, limit=limit, 
                    orderBy=orderBy, groupBy=groupBy)
            self.query=cons
            cursor.execute(cons, whereClause.where_values_list)

        #gets all rows 
        lista = cursor.fetchall()
        if get_rows_as_dicts:
            r=lista[0][0]
        else:
            r=lista
        if r == None:
            r = [] #there wheren't selected rows
        else:
            #in ubuntu 14.04 r is a string, in 16.04 is a list
            #if it is a string is converted to list
            if type(r) is str:
                r=json.loads(r)
        
        if self.global_print_queries or print_query:
            print('pgSelect')
            print("Query: ", cons)
            if whereClause != None:
                whereClause.printProperties()
            print("Num of selected rows: ", len(r))

        return r

    def pgGetTableFieldNames(self, nomTable, changeGeomBySt_asgeojosonGeom=True, nomGeometryField='geom', listOfFieldsToRemove=[],returnAsString=False):
        """
        Retuns a list with the table field names.
        @type  nomTable: string
        @param nomTable: table name included the schema. Ej. "d.linde". 
            Mandatory specify the schema name: public.tablename
        @type  changeGeomBySt_asgeojosonGeom: boolean
        @param changeGeomBySt_asgeojosonGeom: Specifies id the geom name field is changed by st_asgeojson(fieldName).     
        @type  nomGeometryField: string
        @param nomGeometryField: the geometry field name
        @param returnAsString: if true returns the fields in a string 'campo1,campo2,...' 
        @param listOfFieldsToRemove: a list with the fields of the table to remove from the result
        @return: A list with the table field names
    
        Executes the sentence: 
        SELECT column_name FROM information_schema.columns WHERE table_schema='h30' and table_name = 'linde';
        
        Examples of use:
            listaCampos=getTableFieldNames('d.buildings')
                Returns: [u'gid', u'descripcion', u'area', 'st_asgeojson(geom)', u'fecha']
            listaCampos=getTableFieldNames(d.buildings', changeGeomBySt_asgeojosonGeom=False, nomGeometryField='geom')
                Returns: [u'gid', u'descripcion', u'area', u'geom', u'fecha']
        """
        
        consulta="SELECT column_name FROM information_schema.columns WHERE table_schema=%s and table_name = %s";
        lis=nomTable.split(".")
        
        cursor=self.pgConnection.cursor
        cursor.execute(consulta,lis)
        if cursor.rowcount==0:
            return None
            
        listaValores=cursor.fetchall()#es una lista de tuplas.
                #cada tupla es una fila. En este caso, la fila tiene un
                #unico elemento, que es el nombre del campo.
        #print(listaValores)
        listaNombreCampos=[]
        for fila2 in listaValores:
            valor=fila2[0]
            if valor in listOfFieldsToRemove:
                continue
            if changeGeomBySt_asgeojosonGeom:
                if valor==nomGeometryField:
                    valor='st_asgeojson({0})'.format(nomGeometryField)
            listaNombreCampos.append(valor)
        self.query=consulta

        if returnAsString:
            s=""
            for campo in listaNombreCampos:
                s=s + campo + ","
            return s[:-1]  
        else:
            return listaNombreCampos   

    def pgTableExists(self, table_name_with_schema: str, print_query: boolean=False)->Boolean:
        """
        Returns True or False, depending on if the table exists in the database or not.
        table_name_with_schema: table name included the schema. Ej. "d.boundary".
        print_query: For debugging purposes. If true will print the queries. 
    
        Returns: 
            True or False, depending on if the table exists in the database or not.
        """

        l=table_name_with_schema.split(".")
        table_schema=l[0]
        table_name=l[1]
        cons="SELECT EXISTS (SELECT 1 FROM   information_schema.tables WHERE  table_schema = %s AND    table_name = %s)"
        self.pgConnection.cursor.execute(cons,[table_schema, table_name])
        if self.global_print_queries or print_query:
            print('pgTableExists')
            print('Query:', cons)
        r=self.pgConnection.cursor.fetchall()
        return r[0][0]

    def pgCreateTable(self,table_name_with_schema:str, fields_definition:str, 
        delete_table_if_exists: bool, print_query: bool= False)->bool:
        """
        Creates a table. If the table already exists this method can delete it.
        
        See an example of use in 
        <a href="../tutorials/#delete-rows-and-files">Delete rows and files</a>.

        Args:

            table_name_with_schema: The table name, including the schema, e.g. 'public.customers'.
            fields_definition: String with the fields definitions. If any field name stars
                with a number, or has a space, must be quoted, e.g. 
                'gid serial primary key, natcode varchar,nameunit varchar,"{fieldName}" varchar'.format(
                    fieldName='25_utm') 
            delete_table_if_exists: If True will delete the table before create it. If False,
                if the table exists Psycopg2 will raise an error.
            print_query: For debugging purposes. If true the delete and create table
                sentences will be printed.
       
        
        See an example of use in 
        <a href="../tutorials/#delete-rows-and-files">Delete rows and files</a>.
        """
        schema=table_name_with_schema.split(sep='.')[0]
        tableName=table_name_with_schema.split(sep='.')[1]
        if delete_table_if_exists:
            if self.pgTableExists(table_name_with_schema=table_name_with_schema, print_query=print_query):
                cons='drop table "{schema}"."{tableName}"'.format(schema=schema,tableName=tableName)
                self.pgConnection.cursor.execute(cons)
                if self.global_print_queries or print_query:
                    print('pgCreateTable')
                    print("Query to delete the table: ", cons)
        cons='create table "{schema}"."{tableName}" ({fields_definition})'.format(
                    schema=schema,tableName=tableName,fields_definition=fields_definition)
        #print(cons)
        self.pgConnection.cursor.execute(cons)
        self.pgConnection.conn.commit()

        if self.global_print_queries or print_query:
            print('pgCreateTable')
            print("Query to create the table: ", cons)
        return True
        
    def pgValueExists(self, table_name_with_schema, column_name, column_value):
        """
        Returns a true or false depending on if the value exists on the column or not.
        @type  table_name_with_schema: string
        @param table_name_with_schema: table name included the schema. Ej. "d.linde". 
        @type  column_name: string
        @param column_name: column name. Ej. "username". 
        @type  column_value: any
        @param any value in the column. 
        @return: true or false
        """

        cons="SELECT exists (SELECT {0} FROM {1} WHERE {2} = %s LIMIT 1)".format(column_name, table_name_with_schema, column_name)
        self.pgConnection.cursor.execute(cons,[column_value])
        r=self.pgConnection.cursor.fetchall()
        return r[0][0]

    def pgDeleteAllTableRowsFromTableWithColumnValue(self, tableName, columnName, columnValue):
        """
        Deletes all rows from a work from a table. For example:
        deleteAllTableRowsFromTableWithColumnValue(tableName="public.work_images", columnName="color_works_gid", columnValue=25)
        removes all the rows from public.work_images where color_works_gid=25
        """
        return self.pgDelete(table_name=tableName, cond_where=columnName + " =%s", list_values_cond_where=[columnValue])

    def pgIncrementCounter(self, tableName, columnNameToIncrement, cond_where, list_val_cond_where, increment_value=1):
        r=self.pgSelect(table_name=tableName, string_fields_to_select=columnNameToIncrement, cond_where=cond_where,list_val_cond_where=list_val_cond_where)
        if r[0][columnNameToIncrement] is None:
            value=1
        else:
            value=r[0][columnNameToIncrement]+increment_value
        
        oStrFielsAndValues=StrFielsAndValuesBase(str_field_names=columnNameToIncrement,list_field_values=[value], str_s_values="%s")
        numRows=self.pgUpdate(tableName,oStrFielsAndValues,cond_where,list_val_cond_where)
        return numRows