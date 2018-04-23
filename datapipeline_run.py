#!/usr/bin/python

def datapipeline():
	import numpy as np
	import pandas as pd
	from pandas.io import sql
	import mysql.connector as mariadb
	from sqlalchemy import create_engine
	#
	engine = create_engine('mysql+mysqlconnector://wordpress:password@localhost/mydbname')
	connection = engine.connect()
	#
	df = pd.read_csv('/home/victor/Desktop/Atlantic_Test/wordpress/wp-content/uploads/2018/04/inputfile.txt', sep="\t", header = None)
	#print df
	#
	# Use `rename()` to rename your columns
	dfn = df.rename(index=str, columns={
	0: "customer_id",
	1: "customer_fn",
	2: "customer_ln",
	3: "customer_street",
	4: "customer_state",
	5: "customer_zcode",
	6: "purchase_status",
	7: "product_id",
	8: "product_name",
	9: "purchase_amount",
	10: "purchase_date"
	})
	#
	cust_df = dfn[['customer_id', 'customer_fn','customer_ln','customer_street','customer_state','customer_zcode']].copy()
	prod_df = dfn[['product_id', 'product_name']].copy()
	purc_df = dfn[[				'customer_id','product_id','purchase_status','purchase_date','purchase_amount']].copy()
	#purc_df.assign(purchase_id = 0)
	#cust_df.customer_id.unique()
	#
	rows = 0
	connection = engine.connect()
	result = connection.execute("SELECT purchase_id FROM f_purchase;")
	rows = result.fetchall()
	#print(len(rows))
	#
	if len(rows) == 0:
		cust_df.drop_duplicates(subset=None, keep='first', inplace=False).to_sql(name='d_customer', con=engine, if_exists='append', index=False)
		prod_df.drop_duplicates(subset=None, keep='first', inplace=False).to_sql(name='d_products', con=engine, if_exists='append', index=False)
		purc_df.to_sql(name='f_purchase', con=engine, if_exists='append', index=False)
		connection.close()
	#
	if len(rows) > 0:
		print("Data already loaded")
		#Create staging tables
		connection = engine.connect()
		cursor = connection.execute("select * from mydbname.sys_ddl;")
		listdata = []
		listdata = [(i.ddl_create) for i in cursor]
		connection.close()
		for script in listdata:
			#print script
			connection = engine.connect()
			connection.execute(script)
		connection.close()
		#
		cust_df.drop_duplicates(subset=None, keep='first', inplace=False).to_sql(name='d_customer_stg', con=engine, if_exists='append', index=False)
		prod_df.drop_duplicates(subset=None, keep='first', inplace=False).to_sql(name='d_products_stg', con=engine, if_exists='append', index=False)
		purc_df.assign(purchase_id = 0).to_sql(name='f_purchase_stg', con=engine, if_exists='append', index=False)
		#
		query_update = "UPDATE `mydbname`.`sys_triggers` SET `sp_switch` = 1 WHERE `sp_name` = 'update_tables'"
		connection = engine.connect()
		connection.execute(query_update, multi=True)
		connection.close()
		#Drop staging tables
		connection = engine.connect()
		cursor = connection.execute("select * from mydbname.sys_ddl;")
		listdata = []
		listdata = [(i.ddl_drop) for i in cursor]
		for script in listdata:
			#print script
			connection = engine.connect()
			connection.execute(script)
		connection.close()

datapipeline()
