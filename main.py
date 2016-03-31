#!/usr/bin/python
# -*- coding: utf-8 -*-

#
#Author: Pengfei Shi <shipengfei92@gmail.com>
#
from usingNeo4j import *
from neo4jrestclient.client import GraphDatabase
import time
import MySQLdb

start=time.time()
print "start time = %s" %start

connection=usingNeo4j()
#connection.createNode('数据领域','数据领域')
#connection.createNode('数据提供单位','数据领域')
#connection.createRelation('上海市政府数据服务网', '包含','数据领域')
#connection.createRelation('上海市政府数据服务网', '包含','数据提供单位' )
cmd = "CREATE (%s:类型 {name:'%s'})" %('数据领域','数据领域')
connection.execute(cmd)
cmd = "CREATE (%s:类型 {name:'%s'})" %('数据提供单位','数据提供单位')
connection.execute(cmd)

cmd = "MATCH (a:`上海市政府数据服务网`),(b:类型) CREATE (a)-[:包含]->(b) RETURN a,b"
connection.execute(cmd)

db = MySQLdb.connect("ip","neo4j","password","opendata" )
cursor=db.cursor()

cmd = """USE opendata"""

cursor.execute("USE opendata")
cursor.execute("SELECT * FROM item WHERE source_id=9 ")

# Fetch a single row using fetchone() method.
data = cursor.fetchall()
print len(data)

list_field = []
list_units = []

for i in range(0,len(data)):
    url = ((data[i][2].split('^'))[0].split('$'))[1]
    name = ((data[i][2].split('^'))[5].split('$'))[1]
    field = ((data[i][2].split('^'))[4].split('$'))[1]
    list_field.append(field)
    units = ((data[i][2].split('^'))[8].split('$'))[1]
    list_units.append(units)
    #print url,name,field,units
    cmd="CREATE (%s:%s {name:'%s',url:'%s',数据领域:'%s',数据提供单位:'%s'})" % ("data"+str(i),field,name,url,field,units)
    #print cmd
    connection.execute(cmd)
    print i
    
meta_field = list(set(list_field))
meta_units = list(set(list_units))

for i in range(0,len(meta_field)):
    print i
    #print meta_field[i],len(meta_field)
    cmd = "CREATE (%s:数据领域 {name:'%s'})" %(meta_field[i],meta_field[i])
    connection.execute(cmd)
    cmd = "MATCH (a {数据领域:'%s'}),(b {name:'%s'}) CREATE (b)-[:包含]->(a) RETURN a,b" %(meta_field[i],meta_field[i])
    connection.execute(cmd)
    
cmd = "MATCH (a {name:'数据领域'}),(b:数据领域) CREATE (a)-[:包含]->(b) RETURN a,b"
connection.execute(cmd)
    
for i in range(0,len(meta_units)):
    print i
    #print meta_units[i],len(meta_units)
    cmd = "CREATE (%s:数据提供单位 {name:'%s'})" %(meta_units[i],meta_units[i])
    connection.execute(cmd)
    cmd = "MATCH (a {数据提供单位:'%s'}),(b {name:'%s'}) CREATE (b)-[:包含]->(a) RETURN a,b" %(meta_units[i],meta_units[i])
    connection.execute(cmd)

cmd = "MATCH (a {name:'数据提供单位'}),(b:数据提供单位) CREATE (a)-[:包含]->(b) RETURN a,b"
connection.execute(cmd) 
   
    #print data1
    #name = data1[5].split('$')[0]
    #context = data1[5].split('$')[1]
#    print name
    
#    n = gdb.nodes.create(name=name)
        #gdb.nodes.create(category=category,context=context)
        #print category
    
    

# disconnect from server

print "connected"

#result=gbd.query(q=q)

end=time.time()
print "end time = %s" %end
d=end-start

#print result[0]


print "total time = %s" %d