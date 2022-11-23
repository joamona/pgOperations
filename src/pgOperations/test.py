# -*- coding: utf-8 -*-
'''
Created on 1 Feb 2019
@author: Gaspar Mora-Navarro
Polytechnic University of Valencia
joamona@cgf.upv.es
'''

"""
create table d.points (gid serial primary key, description varchar, depth double precision, geom geometry('POINT',25831));
"""

import sys
sys.path.append('/home/joamona/www/apps/desweb/pgOperations')

from src.pgOperations import pgOperations as pg

database="pgoperationtest"
user="postgres"
password="postgres"
host="localhost"
port="5432"

global_print_queries=True

def testDropDatabase():
    pgc=pg.PgConnect(database='postgres', user=user, password=password, host=host,port=port)
    pgdb=pg.PgDatabases(pgConnect=pgc)
    pgdb.dropDatabase('pgoperationstest')
    pgc.disconnect()

def testCreateDatabase():
    pgc=pg.PgConnect(database='postgres', user="postgres", password="postgres", host="localhost",port=5432)
    pgdb=pg.PgDatabases(pgConnect=pgc)
    pgc2=pgdb.createDatabase(databaseName="pgoperationstest",addPostgisExtension=True,closeNewConnection=False)
    pgc2.cursor.execute("create schema d")
    pgc2.cursor.execute("create table d.points (gid serial primary key, description varchar, depth double precision, geom geometry('POINT',25831))")
    pgc2.commit()
    pgc2.disconnect()
    pgc.disconnect()

def testPgConnect():
    pgc=pg.PgConnect(database='pgoperationstest', user="postgres", password="postgres", host="localhost",port=5432)
    pgo=pg.PgOperations(pgConnection=pgc)
    pgc.disconnect()

#testPgConnect()

def insert1():
    #Example without geometry field
    print('Insert with a dict')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", 
            host="localhost", port="5432")
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=global_print_queries)

    d={"description": "water well", "depth": 12.15}
    fieldsAndValues=pg.FieldsAndValues(d=d)
    resp=pgo.pgInsert(table_name="d.points", fieldsAndValues=fieldsAndValues, 
            str_fields_returning="gid")
    print(resp)

def insert2():
    
    print('Example with dictionaries and geometry')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", 
            host="localhost", port="5432")
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=global_print_queries)

    d={"description": "water well", "depth": 12.15, "geom": "POINT(100 200)"}
    geometryFieldOptions=pg.GeometryFieldOptions(geom_field_name="geom", 
            epsg='25830',epsg_to_reproject="25831")
    fieldsAndValues=pg.FieldsAndValues(d=d, list_fields_to_remove=["depth"], 
            geometryFieldOptions=geometryFieldOptions)
    resp=pgo.pgInsert(table_name="d.points", fieldsAndValues=fieldsAndValues, 
            str_fields_returning="gid")
    print(resp)
    """
    Result:
        pgInsert
        insert into d.points (description,geom) values (%s,st_transform(st_geometryfromtext(%s,25830),25831)) returning gid
        Values ['water well', 'POINT(100 200)']
        description,geom
        ['water well', 'POINT(100 200)']
        %s,st_transform(st_geometryfromtext(%s,25830),25831)
        [{'gid': 2}]
    """

def insert3():
    print('Example without dictionaries')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", 
            host="localhost", port="5432")
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=global_print_queries)
    fieldsAndValuesBase=pg.FieldsAndValuesBase(
        str_field_names="depth, description, geom", 
        list_field_values=[12.15, "water well","POINT(100 200)"], 
        str_s_values="%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)")
    
    resp=pgo.pgInsert(table_name="d.points", fieldsAndValues=fieldsAndValuesBase, 
            str_fields_returning="gid")
    print(resp)
    oCon.disconnect()
    """
    Result 
    Connected
    Inserting
    insert into d.points (depth, description, geom) values (%s,%s,st_transform(st_geometryfromtext(%s,25830),25831)) returning gid
    [(1,)]
    """

def update1():
    print("Example using a Python dictionary")
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=global_print_queries)
    #update the point gid=1 to this new values
    #you can omit the values not to update, or set the parameter list_fields_to_remove
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
    oCon.disconnect()
    print (resp)
    """
    Result
    
    Query: update d.points set (geom,description) = (st_transform(st_geometryfromtext(%s,25830),25831),%s) where gid=%s
    1
    """

def update2():
    """
    print('Manually creating the expressions')
    """
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=global_print_queries)
    #update the point gid=1 to this new values
    #you can omit the values not to update, or set the parameter list_fields_to_remove
    fieldsAndValuesBase=pg.FieldsAndValuesBase(
        str_field_names="description, geom", 
        list_field_values=["water well updated","POINT(300 300)"], 
        str_s_values="%s,st_transform(st_geometryfromtext(%s,25830),25831)")
    whereClause=pg.WhereClause(where_clause="gid=%s", where_values_list=[1])
    resp=pgo.pgUpdate(table_name="d.points", 
                      fieldsAndValues=fieldsAndValuesBase, 
                      whereClause=whereClause
                      )
    oCon.disconnect()
    print (resp)    
    
    """
    Result

    Query: update d.points set (description, geom) = (%s,st_transform(st_geometryfromtext(%s,25830),25831)) where gid=%s
    1
    """

  
def select():
    print('Select 1. Getting dictionaries')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=global_print_queries)
    whereClause=pg.WhereClause(where_clause='gid > %s and gid < %s', where_values_list=[0, 3])
    resp=pgo.pgSelect(table_name="d.points", 
                      string_fields_to_select='gid,depth,description,st_astext(geom)',
                      whereClause=whereClause,
                      print_query=False,
                      orderBy='gid desc'
                      )
    oCon.disconnect()
    print (resp)
 
def select2():
    print('Select 1. Getting tuples')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=global_print_queries)
    whereClause=pg.WhereClause(where_clause='gid > %s and gid < %s', where_values_list=[0, 3])
    resp=pgo.pgSelect(table_name="d.points", 
                      string_fields_to_select='gid,depth,description,st_astext(geom)', 
                      whereClause=whereClause,
                      print_query=False,
                      orderBy='gid desc',
                      get_rows_as_dicts=False
                      )
    oCon.disconnect()
    print (resp)
    pgo.pgConnection.disconnect() 

def delete():
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=global_print_queries)
    whereClause=pg.WhereClause(where_clause='gid < %s', where_values_list=[3])
    resp=pgo.pgDelete(table_name="d.points",whereClause=whereClause,print_query=False)
    print(resp)
    pgo.pgConnection.disconnect() 

def deleteFileInRow():
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon,global_print_queries=global_print_queries)
    d={'gid': 25, 'description': 'customer', 'img': '00012325.jpg'}
    resp=pgo.pgDeleteFileInRow(row=d,field_name_with_file_name='img',base_path='/home/joamona/temp/img')
    print(resp)
    pgo.pgConnection.disconnect() 

def createTableCustomers():
    print('createTableCustomers')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    r=pgo.pgCreateTable(table_name_with_schema="d.customers",
            fields_definition="gid serial, name varchar, img varchar",
            delete_table_if_exists= True,print_query=False)  
    print(r)
    pgo.pgConnection.disconnect() 

def insertCustomers():
    print('Insert Customers')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
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
    pgo.pgConnection.disconnect()  
    print("Done")

def deleteWithFiles():
    print('Delete with files')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    r=pgo.pgDeleteWithFiles(table_name="d.customers",field_name_with_file_name="img",
        base_path="/home/joamona/temp/img",  print_query=False)
    pgo.pgConnection.disconnect() 
    print(r)

def getTableFieldNames1():
    print('getTableFieldNames. Todos los campos')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    r=pgo.pgGetTableFieldNames('d.points')
    print('Field names: ', r)
    pgo.pgConnection.disconnect() 

def getTableFieldNames2():
    print('getTableFieldNames. Todos los campos menos description')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    r=pgo.pgGetTableFieldNames('d.points',list_fields_to_remove=['description'])
    print('Field names: ', r)
    pgo.pgConnection.disconnect() 

def getTableFieldNames3():
    print('getTableFieldNames. Todos los campos menos description, Obteniendo la geometría como geojson')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    gf=pg.SelectGeometryFormat()
    gfo=pg.SelectGeometryFieldOptions(geom_field_name='geom',select_geometry_format=gf.geojson, epsg_to_reproject='25831')
    r=pgo.pgGetTableFieldNames('d.points',gfo,list_fields_to_remove=['description'],returnAsString=True)
    print('Field names: ', r)
    pgo.pgConnection.disconnect() 


def getTableFieldNames4():
    print('getTableFieldNames. Todos los campos menos description, Obteniendo la geometría como geojson reproyectada')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    gf=pg.SelectGeometryFormat()
    gfo=pg.SelectGeometryFieldOptions(geom_field_name='geom',select_geometry_format=gf.geojson, epsg_to_reproject='25831')
    fieldNames=pgo.pgGetTableFieldNames('d.points',gfo,list_fields_to_remove=['description'],returnAsString=True)
    print('Field names: ', fieldNames)
    wc=pg.WhereClause(where_clause='gid=%s',where_values_list=[3])
    res=pgo.pgSelect(table_name='d.points', string_fields_to_select=fieldNames,whereClause=wc)
    print('Selection result: ', res)
    pgo.pgConnection.disconnect() 

def tableExists():
    print('Table exists')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    res=pgo.pgTableExists(table_name_with_schema='d.points')
    print('Table exists: ', res)
    pgo.pgConnection.disconnect()    

def valueExists():
    print('Value exists')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    res=pgo.pgValueExists(table_name_with_schema='d.points',field_name='gid',field_value= 3)
    print('Value exists:', res)
    pgo.pgConnection.disconnect()   

def addCounter(counter_name):
    print('Add counter')
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    c=pg.PgCounters(pgo)
    c.addCounter(counter_name,counter_name + ' description')
    v1=c.incrementCounter(counter_name)
    print('Returned value 1: ',v1)
    v2=c.incrementCounter(counter_name)
    print('Returned value 2: ',v2)
    r1=c.getCounterValue(counter_name)
    print('Returned value 3: ',r1)
    r=c.getAllCounters()
    print('All counters: ',r)
    pgo.pgConnection.disconnect()  

def deleteCounter(counter_name):
    oCon=pg.PgConnect(database="pgoperationstest", user="postgres", password="postgres", host="localhost", port="5432")  
    pgo=pg.PgOperations(pgConnection=oCon, global_print_queries=global_print_queries)
    c=pg.PgCounters(pgo)
    n=c.deleteCounter(counter_name)

if __name__=="__main__":
    """
    The performed PgOperations tests
    """
    testDropDatabase()
    testCreateDatabase()
    insert1()
    print("------------------------------------")
    insert2()
    print("------------------------------------")
    insert3()
    print("------------------------------------")
    update1()
    print("------------------------------------")
    update2()
    print("------------------------------------")
    select()
    print("------------------------------------")
    select2()
    print("------------------------------------")
    delete()
    print("------------------------------------")
    deleteFileInRow()
    print("------------------------------------")
    createTableCustomers()
    print("------------------------------------")
    insertCustomers()
    print("------------------------------------")
    deleteWithFiles()
    getTableFieldNames1()
    getTableFieldNames2()
    getTableFieldNames3()
    getTableFieldNames4()
    tableExists()
    valueExists()
    addCounter('c1')
    addCounter('c2')
    deleteCounter('c1')
    deleteCounter('c2')
    deleteCounter('c3')
    
