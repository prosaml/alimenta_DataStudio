#Importo las librerias
import math
import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime
import progressbar
import teradata
import requests
import os
import json

os.system('clear')

class bcolors:
	HEADER = '\033[95m'
	OKBLUE = '\033[94m'
	OKGREEN = '\033[92m'
	WARNING = '\033[93m'
	FAIL = '\033[91m'
	ENDC = '\033[0m'
	BOLD = '\033[1m'
	UNDERLINE = '\033[4m'

def creden():
	import gspread
	from oauth2client.service_account import ServiceAccountCredentials
	from df2gspread import df2gspread as d2g
	from open_sheet import open_sheet

	scope_gdoc = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	cre = ServiceAccountCredentials.from_json_keyfile_name('', scope_gdoc)
	gc = gspread.authorize(cre)

	return cre, gc, open_sheet, d2g

#Funcion para sacar los dominios de la api de disponibilización de dominios
def get_dispo(dom,sta):
	df = pd.DataFrame()
	count = 0
	productos = requests.get('https://internal-api.mercadolibre.com/internal/catalog-available-products/products-status?domainIds='+str(dom)+'&status='+str(sta)+'&size=300&page='+str(count))
	availables = productos.json()
	n = math.ceil(availables['total'] / 300)
	while count <= n:
		productos = requests.get('https://internal-api.mercadolibre.com/internal/catalog-available-products/products-status?domainIds='+str(dom)+'&status='+str(sta)+'&size=300&page='+str(count))
		availables = productos.json()
		df_ava = pd.DataFrame(availables['results'])
		df = pd.concat([df,df_ava]).reset_index(drop = True)
		count = count + 1
	   
	return df

def get_product(ids):
	s = requests.Session()
	url = "https://internal-api.mercadolibre.com/products/"+str(ids)
	headers = { "Content-Type": "application/json", "X-Caller-Scopes": "admin"}
	y = s.get(url, headers=headers)
	s.close()
	return y

def get_decorations(ids):
	s = requests.Session()
	url = "https://internal-api.mercadolibre.com/internal/pdp/decorations/decorations/"+str(ids)
	headers = { "Content-Type": "application/json", "X-Caller-Scopes": "admin"}
	y = s.get(url, headers=headers)
	s.close()
	return y

def get_items_matcheados(ids):
	s = requests.Session()
	url = "https://internal-api.mercadolibre.com/items-search-proxy/items?product_id="+str(ids)+"&include_family=true"
	headers = { "Content-Type": "application/json"}
	y = s.get(url, headers=headers)
	s.close()
	return y

def get_status_child(parent, child):
	s = requests.Session()
	url = 'https://internal-api.mercadolibre.com/internal/catalog-available-products/products-status/'+str(parent)+'/children/'+str(child)
	headers = { "Content-Type": "application/json"}
	y = s.get(url, headers=headers)
	s.close()
	return y

#Cuenta short_description
def count_short_desc(prod):
		deco = prod.get('short_description').get('content')
		if deco.count('\n\n') == 0:
			if not deco.isspace():
				if deco.split('\n\n')[len(deco.split('\n\n'))-1].isspace():
					count_paragraph = 0
				elif deco.split('\n\n')[len(deco.split('\n\n'))-1]== '' :
					count_paragraph = len(deco.split('\n\n'))-1
				else:
					count_paragraph = 1
			else:
				count_paragraph = 0
		else:
			if deco.split('\n\n')[len(deco.split('\n\n'))-1].isspace():
				count_paragraph = len(deco.split('\n\n'))-1
			elif deco.split('\n\n')[len(deco.split('\n\n'))-1]== '' :
				count_paragraph = len(deco.split('\n\n'))-1
			else:
				count_paragraph = len(deco.split('\n\n'))
		return count_paragraph

def body_products(prod, context_id):
	# Estructura Body a impactar en API
	body = {
		"equals": [{
			"path": "catalog_product_id",
			"value": prod
		}, {
			"path": "catalog_listing",
			"value": True
		}],
		"match_any": [
			{
				"not_equals": [
					{
						"path": "status",
						"value": "closed"
					},
					{
						"path": "status",
						"value": "under_review"
					},
					{
						"path": "status",
						"value": "inactive"
					}
				]
			}
		],
		"fields": [
			"id",
			"catalog_product_id",
			"catalog_listing",
			"status"
		],
		"size": 1000,
		"context_id": ""
		}
	
	body['context_id'] = context_id
		
	return json.dumps(body)

def fecha_ids_status(df,fecha):
	prod_ids = []
	for i in range(len(df)):
		logs = df.loc[i,'logs']
		if datetime.strftime(datetime.strptime(logs[0]['date_created'], '%Y-%m-%dT%H:%M:%SZ'), '%Y-%m-%d') >= fecha:
			prod_ids.append({'product_id': df.loc[i,'product_id']
							})

	prod_ids_df = pd.DataFrame(prod_ids).fillna('').reset_index(drop=True)
	return prod_ids_df

def post_batch(produ):
	df = pd.DataFrame()
	context_id = ''
	bd_item = body_products(produ, context_id)
	s = requests.Session()
	url = "https://internal-api.mercadolibre.com/items-batch-search/ds/scroll_search"
	headers = { "Content-Type": "application/json"}
	p = s.post(url, data=bd_item, headers=headers)
	availables = p.json()
	while (availables['documents'] != []) and (availables['total'] > 1000):
			df_ava = pd.DataFrame(availables['documents'])
			df = pd.concat([df,df_ava]).reset_index(drop = True)
			context_id = availables['context_id']
			bd_item = body_products(produ, context_id)
			url = "https://internal-api.mercadolibre.com/items-batch-search/ds/scroll_search"
			headers = { "Content-Type": "application/json"}
			p = s.post(url, data=bd_item, headers=headers)
			availables = p.json()
	
	if df.empty:
		df = pd.DataFrame(availables['documents']) if availables['documents'] != [] else df
		return df
	else:
		return df
	s.close()

	
def matcheo(ids):
    s = requests.Session()
    url = "https://internal-api.mercadolibre.com/items-search-proxy/items?product_id="+str(ids)+"&include_family=true"
    headers = { "Content-Type": "application/json", "X-Caller-Scopes": "admin"}
    y = s.get(url, headers=headers)
    s.close()
    return y

def query(dominio):
	username_teradata = os.getenv('USER_TERADATA')
	password_teradata = os.getenv('PASS_TERADATA')
	host_teradata = os.getenv('HOST_TERADATA')

	#Contectamos a Teradata
	udaExec = teradata.UdaExec(appName="MyApp", version="1.0", logConsole=False)

	session = udaExec.connect(method="odbc", 
							 system=""+host_teradata+"", 
							 username= ""+username_teradata+"", 
							 password=""+password_teradata+"", 
							 authentication="LDAP",
							 USEREGIONALSETTINGS='N',
							 driver="Teradata Database ODBC Driver 17.00",
							 charset='UTF8')

	#Query para saber el GMV de todos los productos (CTLG_PROD_ID = )
	gmv_producto = pd.read_sql_query(""" SELECT distinct(C.CTLG_PROD_ID), C.sit_site_id, SUBSTRING(C.dom_domain_id,5) as DOMINIO,
	SUM(case when C.CTLG_PROD_ID is not null then C.BID_BASE_CURRENT_PRICE * C.BID_QUANTITY_OK end) AS GMV_CHILD
	FROM WHOWNER.BT_BIDS C
	WHERE C.ITE_GMV_FLAG = 1
	AND C.MKT_MARKETPLACE_ID = 'TM'
	AND C.sit_site_id in ('MLA', 'MLB', 'MLM')
	AND C.TIM_DAY_WINNING_DATE BETWEEN ADD_MONTHS(DATE,-1) AND DATE
	AND C.photo_id = 'TODATE'
	AND SUBSTRING(C.dom_domain_id,5) in (SELECT P.dom_domain_id FROM WHOWNER.LK_BUYBOX_PRODUCT_AVAILABLE P GROUP BY 1)
	AND SUBSTRING(C.dom_domain_id,5) = '"""+dominio+"""'
	AND C.CTLG_PROD_ID is not null
	GROUP BY 1,2,3;""", session)
	gmv_producto['CTLG_PROD_ID'] = gmv_producto['CTLG_PROD_ID'].astype(int)
	gmv_producto['ID'] = gmv_producto.SIT_SITE_ID.astype(str).str.cat(gmv_producto.CTLG_PROD_ID.astype(str))

	#Query para sacar los dominios productizados (las ventas de los items tienen asociadas un producto del catalogo)
	gmv_dominio = pd.read_sql_query(""" SELECT C.sit_site_id, SUBSTRING(C.dom_domain_id,5) as DOMINIO,
	SUM(case when C.CTLG_PROD_ID is not null then C.BID_BASE_CURRENT_PRICE * C.BID_QUANTITY_OK end) AS GMV_DOM
	FROM WHOWNER.BT_BIDS C
	WHERE C.ITE_GMV_FLAG = 1
	AND C.MKT_MARKETPLACE_ID = 'TM'
	AND C.sit_site_id in ('MLA', 'MLB', 'MLM')
	AND C.TIM_DAY_WINNING_DATE BETWEEN ADD_MONTHS(DATE,-1) AND DATE
	AND C.photo_id = 'TODATE'
	AND SUBSTRING(C.dom_domain_id,5) in (SELECT P.dom_domain_id FROM WHOWNER.LK_BUYBOX_PRODUCT_AVAILABLE P GROUP BY 1)
	AND SUBSTRING(C.dom_domain_id,5) = '"""+dominio+"""'
	AND C.CTLG_PROD_ID is not null
	GROUP BY 1,2;""",session)
	
	df_gmv = gmv_producto.merge(gmv_dominio, left_on='SIT_SITE_ID', right_on='SIT_SITE_ID', how='left')
	df_gmv = df_gmv[['ID','GMV_CHILD','GMV_DOM']]
	df_gmv['%GMV'] = df_gmv.apply(lambda row: (row['GMV_CHILD']/row['GMV_DOM']), axis=1)

	return df_gmv

def tagueo(df_total, pic_ideal, mf_ideal, sd_ideal,df_gmv,status):
	#Defino el peso de las variables
	w_pic = float(0.50)
	w_mf = float(0.35)
	w_sd = float(0.15)

	#Agrego la fecha de revisión
	d = datetime.today()

	for x in progressbar.progressbar(range(len(df_total))):
		df_total.loc[x,'Score'] = float(round(df_total.loc[x,'Pictures']/pic_ideal*w_pic + df_total.loc[x,'Main_features']/mf_ideal*w_mf+df_total.loc[x,'Short_description']/sd_ideal*w_sd,3))
		df_total.loc[x,'PICTURE_IDEAL'] = pic_ideal
		df_total.loc[x,'MAIN_FEATURES_IDEAL'] = mf_ideal
		df_total.loc[x,'SHORT_DESSCRIPTION_IDEAL'] = sd_ideal
		df_total.loc[x, 'Fecha de Revisión'] = d.strftime('%d-%m-%Y')
		
		#Armo los rangos de los tags según dominio
		
		df_total.loc[x,'A_DOMAIN'] = 1
		df_total.loc[x,'B_DOMAIN'] = float((pic_ideal-1)/pic_ideal*w_pic+ (mf_ideal-2)/mf_ideal*w_mf+(sd_ideal-2)/sd_ideal*w_sd)
		
		#Tagueo
		
		if df_total.loc[x,'Pictures']== 0 or df_total.loc[x,'Main_features'] <2 or df_total.loc[x,'Short_description']==0:
			df_total.loc[x,'tag'] = 'INACEPTABLE'

		elif df_total.loc[x,'Pictures'] >= pic_ideal and df_total.loc[x,'Main_features'] >= mf_ideal and df_total.loc[x,'Short_description'] >= sd_ideal:
			df_total.loc[x,'tag'] = 'A'

		elif df_total.loc[x,'Score'] >= df_total.loc[x,'B_DOMAIN']:
				df_total.loc[x,'tag'] = 'B'

		elif df_total.loc[x,'Pictures'] >= 1 and df_total.loc[x,'Main_features'] >= 2 and df_total.loc[x,'Short_description'] >= 1:
				df_total.loc[x,'tag'] = 'C'
		else:
			df_total.loc[x,'tag'] = 'INACEPTABLE'
		
		#Tague Picture
		if df_total.loc[x,'Pictures']>= df_total.loc[x,'PICTURE_IDEAL']:
			df_total.loc[x,'tag_pictures'] = 'A'
		elif df_total.loc[x,'Pictures'] >= 0.7*(df_total.loc[x,'PICTURE_IDEAL']):
			df_total.loc[x,'tag_pictures'] = 'B'
		elif df_total.loc[x,'Pictures'] >=1:
			df_total.loc[x,'tag_pictures'] = 'C'
		else:
			df_total.loc[x,'tag_pictures'] = 'INACEPTABLE'
		
		#Tague Contenido
		if df_total.loc[x,'Main_features'] >= mf_ideal and df_total.loc[x,'Short_description'] >= sd_ideal:
			df_total.loc[x,'tag_contenido'] = 'A'
		else:
			if (((df_total.loc[x,'Main_features']/mf_ideal)*0.7)+((df_total.loc[x,'Short_description']/sd_ideal)*0.3))>0.6:
				df_total.loc[x,'tag_contenido'] = 'B'
			
			elif df_total.loc[x,'Main_features'] >= 2 and df_total.loc[x,'Short_description'] >= 1:
				df_total.loc[x,'tag_contenido'] = 'C'
			else:
				df_total.loc[x,'tag_contenido'] = 'INACEPTABLE'

		#Status del hijo
		if df_total.loc[x,'Id'] != '':
			status_child = get_status_child(df_total.loc[x,'Parent_id'][3::], df_total.loc[x,'Id'][3::])
			if status_child.status_code in range(200,300):
				df_total.loc[x,'Status_child'] = status_child.json().get('status')

			#Agrego el GMV
			if df_total.loc[x,'Id'] in df_gmv.ID.to_list():
				df_total.loc[x,'%GMV'] = round(float(df_gmv[df_gmv['ID'] == str(df_total.loc[x,'Id'])].get('%GMV')),3) if not df_gmv[df_gmv['ID'] == str(df_total.loc[x,'Id'])].empty else ''

			#cantidad de optins
			#df_total.loc[x,'Q_optins']= int(len(post_batch(df_total.loc[x,'Id'])))
			
			#Separo ste / id
			df_total.loc[x,'site'] = df_total.loc[x,'Id'][:3]
			df_total.loc[x,'id'] = df_total.loc[x,'Id'][3::]
			
			#Total de matcheos
			match = matcheo(df_total.loc[x,'Id'])
			if match.status_code in range(200,300):
				df_total.loc[x,'q_matcheos'] = match.json().get('total')
			else:
				df_total.loc[x,'q_matcheos'] = ''
		
		else:
			df_total.loc[x,'%GMV'] = round(float(df_gmv[df_gmv['ID'] == str(df_total.loc[x,'Parent_id'])].get('%GMV')),3) if not df_gmv[df_gmv['ID'] == str(df_total.loc[x,'Parent_id'])].empty else ''
			#df_total.loc[x,'Q_optins']= int(len(post_batch(df_total.loc[x,'Parent_id'])))
			df_total.loc[x,'Status_child'] = status
			df_total.loc[x,'site'] = df_total.loc[x,'Parent_id'][:3]
			df_total.loc[x,'id'] = df_total.loc[x,'Parent_id'][3::]
			match = matcheo(df_total.loc[x,'Parent_id'])
			
			if match.status_code in range(200,300):
				df_total.loc[x,'q_matcheos'] = match.json().get('total')
			else:
				df_total.loc[x,'q_matcheos'] = ''
	df_total.rename(columns={df_total.columns[0]: 'Hijo_id'}, inplace=True)

	if len(df_total[df_total['tag']== 'INACEPTABLE'])!= 0:
		for y in range(len(df_total)):
			if df_total.loc[y,'Main_features']== 0 and df_total.loc[y,'Short_description']== 0:
				deco_child = get_decorations(df_total.loc[y,'Hijo_id']) 
				if deco_child.status_code in range(200,300):
					deco_child = deco_child.json()
					if deco_child.get('short_description').get('content') != '' and deco_child.get('main_features') != None:
						df_total.loc[y,'AVISO'] = 'El id tiene decoracion en la API de decorations.'
					elif deco_child.get('short_description').get('content') == '' and deco_child.get('main_features') != None:
						df_total.loc[y,'AVISO'] = 'El id tiene decoracion en la API de decorations.'
					elif deco_child.get('short_description').get('content') != '' and deco_child.get('main_features') == None:
						df_total.loc[y,'AVISO'] = 'El id tiene decoracion en la API de decorations.'

	if "%GMV" not in df_total.columns:
		df_total.insert(6, "%GMV",'')
	
	if 'AVISO' not in df_total.columns:
		df_total.insert(20, 'AVISO','')

	return df_total

def info(dominio):
	print('Procesando... ')
	status = 'CATALOG_DONE'
	ids = get_dispo(dominio,status)
	lis1 =[]
	procesados = []
	sites = ['MLA','MLB','MLM']
	print("Extrayendo la info de : "+dominio)
	for i in progressbar.progressbar(range(len(ids))):
		for j in sites:
			parent = get_product(str(str(j)+str(ids.loc[i,'product_id'])))
			if parent.status_code in range(200,300):
				parent = parent.json()
				if parent.get('children_ids')!= []: #Verdadero = Tiene hijos
					for ch in range(len(parent.get('children_ids'))):
						child = get_product(parent.get('children_ids')[ch]) #Hijos del padre
						if child.status_code in range(200,300):
							child = child.json()
							lis1.append({'Id': child.get('id'),
										 'Status': child.get('status'),
										 'Name': child.get('name'),
										 'Parent_id': str(str(j)+str(ids.loc[i,'product_id'])),
										 'Pictures' : len(child.get('pictures')) if child.get('pictures') != None else int(0),
										 'Main_features' : len(child.get('main_features')) if child.get('main_features') != None else 0,
										 'Short_description': int(count_short_desc(child)) if child.get('short_description').get('content') != '' else 0,
										 'Domain' : dominio
										 })
							procesados.append(child.get('id'))
								
					pd.to_pickle(procesados,'./Backup/bck_'+str(dominio))
					pd.to_pickle(procesados,'./Backup/bck_'+str(dominio))
				else: #Padre sin hijos
					lis1.append({'Id': '',
								 'Status': parent.get('status'),
								 'Name': parent.get('name'),
								 'Parent_id': str(str(j)+str(ids.loc[i,'product_id'])),
								 'Pictures' : len(parent.get('pictures')) if parent.get('pictures') != None else int(0),
								 'Main_features' : len(parent.get('main_features')) if parent.get('main_features') != None else 0,
								 'Short_description': int(count_short_desc(parent)) if parent.get('short_description').get('content') != '' else 0,
								 'Domain' : dominio
								 })
					procesados.append(parent.get('id'))
					
					time.sleep(2)


	df_total = pd.DataFrame(lis1).reset_index(drop = True)
	df_total = df_total[['Id', 'Parent_id','Name' ,'Domain','Status','Pictures', 'Main_features','Short_description']]

	return df_total


def imprimir():
	print("\n\x1b[0;36mOpciones disponibles:\x1b[0;37m")
	print("\t1. Actualizar un dominio")
	print("\t2. Actualizar un dominio a partir de una fecha")
	print()

def menu():
	imprimir()
	entrada_usuario = int(input("Seleccione una opcion: "))
	if entrada_usuario in range(4):
		status = 'CATALOG_DONE'
		if entrada_usuario == 1:

			dominio = str(input('Ingrese el dominio:')).upper()

			pic_ideal = int(input("Agregar la cantidad de PICTURES ideal de "+dominio +": "))
			mf_ideal = int(input("Agregar la cantidad de MAIN_FEATURES ideal de "+dominio+": "))
			sd_ideal = int(input("Agregar la cantidad de SHORT_DESCRIPTIONS ideal de "+dominio+": "))

			df_info = info(dominio)
			
			df_gmv = query(dominio)

			df_total = tagueo(df_info, pic_ideal, mf_ideal, sd_ideal,df_gmv, status)

			df_total= df_total[['Fecha de Revisión','Hijo_id', 'Parent_id', 'Name', 'Domain', 'Status' ,'%GMV', 'Pictures',
					'Main_features', 'Short_description','Score', 'tag','tag_contenido', 'tag_pictures',
					'PICTURE_IDEAL','MAIN_FEATURES_IDEAL','SHORT_DESSCRIPTION_IDEAL', 'A_DOMAIN' , 
					'B_DOMAIN', 'AVISO', 'Status_child','site','id', 'q_matcheos']]

			credentials, gc, open_sheet, d2g = creden()

			Planilla = input("\nIngrese ID de planilla para guardar el analisis: ")

			d2g.upload(df_total.fillna(''), Planilla, str('Calidad_'+ dominio), credentials=credentials, row_names=False, new_sheet_dimensions=(len(df_total), len(df_total.columns)))

			print("\x1b[0;33m\nSe creo la hoja Calidad_"+str(dominio)+"\x1b[0;37m")

		elif entrada_usuario == 2:
			
			dominio = input(bcolors.HEADER + "Ingrese el dominio que desea actualizar "+bcolors.ENDC)
			date = input(bcolors.HEADER + "Ingrese la fecha a analizar, formato YYYY-MM-DD: "+bcolors.ENDC)

			pic_ideal = int(input("Agregar la cantidad de PICTURES ideal de "+dominio +": "))
			mf_ideal = int(input("Agregar la cantidad de MAIN_FEATURES ideal de "+dominio+": "))
			sd_ideal = int(input("Agregar la cantidad de SHORT_DESCRIPTIONS ideal de "+dominio+": "))


			Planilla = input(bcolors.HEADER+"Ingrese ID de planilla para guardar el analisis: "+bcolors.ENDC)
			print(bcolors.OKBLUE +'Analizando ' +dominio+bcolors.ENDC)
			df_gmv = query(dominio)
			status = 'CATALOG_DONE'
			ids = get_dispo(dominio,status)
			ids = fecha_ids_status(ids,date)
			lis1 = []
			sites = ['MLA','MLB','MLM']
			for i in progressbar.progressbar(range(len(ids))):
				for s in sites:
					parent = get_product(str(str(s)+str(ids.loc[i,'product_id'])))
					if parent.status_code in range(200,300):
						parent = parent.json()
						if parent.get('children_ids')!= []: #Verdadero = Tiene hijos
							for ch in range(len(parent.get('children_ids'))):
								child = get_product(parent.get('children_ids')[ch]) #Hijos del padre
								if child.status_code in range(200,300):
									child = child.json()
									lis1.append({'Id': child.get('id'),
												 'Status': child.get('status'),
												 'Name': child.get('name'),
												 'Parent_id': child.get('parent_id'),
												 'Pictures' : len(child.get('pictures')) if child.get('pictures') != None else int(0),
												 'Main_features' : len(child.get('main_features')) if child.get('main_features') != None else 0,
												 'Short_description': int(count_short_desc(child)) if child.get('short_description').get('content') != '' else 0,
												 'Domain' : dominio
												 })
						else: #Padre sin hijos
							lis1.append({'Id': '',
										 'Status': parent.get('status'),
										 'Name': parent.get('name'),
										 'Parent_id': parent.get('id'),
										 'Pictures' : len(parent.get('pictures')) if parent.get('pictures') != None else int(0),
										 'Main_features' : len(parent.get('main_features')) if parent.get('main_features') != None else 0,
										 'Short_description': int(count_short_desc(parent)) if parent.get('short_description').get('content') != '' else 0,
										 'Domain' : dominio
										 })
							time.sleep(2)
			
			if lis1 != []:
				df_total = pd.DataFrame(lis1).reset_index(drop = True)[['Id', 'Parent_id','Name' ,'Domain',
																	'Status','Pictures','Main_features',
																	'Short_description']]
			
				#Conectar a las apis de google
				credentials, gc, open_sheet, d2g = creden()

				producto_ = '1wIanRUnVEM552BUokSjDVWxMB8SVybRGili826j96GM'
				ideales = open_sheet('DOMINIOS 35+GMV',producto_,gc).reset_index(drop = True)
				base = open_sheet('TAG',producto_,gc).reset_index(drop = True)
				
				tagueo(df_total, pic_ideal, mf_ideal, sd_ideal,df_gmv,status)

				for c in range(len(df_total)):
					id_ = df_total.loc[c,'Hijo_id']
					if base[base['Hijo_id'] == id_].empty:
						df_total.loc[c,'Producto nuevo'] = 'SI'
						df_total.loc[c,'Viejo Score'] = ''
						df_total.loc[c,'Viejo Tag'] = ''
					else:
						df_total.loc[c,'Producto nuevo'] = 'NO'
						df_total.iloc[c]['Viejo Score'] = base[base['Hijo_id'] == id_]['Score']
						df_total.iloc[c]['Viejo Tag'] = base[base['Hijo_id'] == id_]['tag']

				if 'AVISO' not in df_total.columns:
					df_total.insert(20, 'AVISO','')
				if 'Viejo Score' not in df_total.columns:
					df_total.insert(27, 'Viejo Score','')
				if 'Viejo Tag' not in df_total.columns:
					df_total.insert(28, 'Viejo Tag','')

				df_total= df_total[['Fecha de Revisión','Hijo_id', 'Parent_id', 'Name', 'Domain', 'Status' ,'%GMV', 'Pictures',
							'Main_features', 'Short_description','Score', 'tag','tag_contenido', 'tag_pictures',
							'PICTURE_IDEAL','MAIN_FEATURES_IDEAL','SHORT_DESSCRIPTION_IDEAL', 'A_DOMAIN' , 
							'B_DOMAIN', 'AVISO', 'Status_child','site','id', 'q_matcheos','Producto nuevo',
								'Viejo Score' , 'Viejo Tag']]

				d2g.upload(df_total.fillna(''), Planilla, str('Calidad_'+ dominio), credentials=credentials, row_names=False, new_sheet_dimensions=(len(df_total), len(df_total.columns)))
				print("\x1b[0;33m\nSe creo la hoja Calidad_"+dominio+"\x1b[0;37m")
			else:
				print(bcolors.FAIL+ "El dominio "+dominio+" no recibió actualizaciones"+bcolors.ENDC)
	else:
		print(bcolors.FAIL+ "Opción no válida"+bcolors.ENDC)
if __name__ == '__main__':
	menu()