from sqlalchemy import create_engine
import pandas as pd
import pickle
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from Modules.config import *
import math
import numpy as np


engine = create_engine(
	"postgresql://{}:{}@{}:{}/{}".format(
		"awsdevuser",
		"Jakarta123456789",
		"sig-jkt-dev-redshift-cluster1.cqjg3vge9uvi.ap-southeast-3.redshift.amazonaws.com",
		"5439",
		"dev",
	)
)


def get_simulation_data(engine,tabel_simulation, simulation_status):
	statement = """
	select * from {0!s} where simulation_status = '{1!s}'
	""".format(tabel_simulation, simulation_status)
	data_simulation = pd.read_sql_query(statement, engine)
	return data_simulation


def update_simulation_data(engine, data_simulation, status, simulation_table):
	data_simulation['simulation_status'] = status
	data_simulation.to_sql(simulation_table, engine, index=False, if_exists='replace')
	return "update status done"


def get_flag_change(data_simulation, flag):
	data_flag = data_simulation[data_simulation['flag_change']==flag]
	return data_flag


def grouping_entity(entity,material):
	if entity == 'SBA':
		return "ANDALAS"
	elif entity == 'SBI':
		if material == 'PCC PREMIUM':
			return "POWERMAX"
		elif material == "MASONRY":
			return "MASONRY"
		else:
			return "DYNAMIX"
	else:
		return entity


def prep_cbp_modelling_retail(data_simulation, data_predict):
	data_simulation_retail = get_flag_change(data_simulation,'Price')
	# data_simulation_retail['brand_name'] = list(map(lambda x,y: grouping_entity(x,y),data_simulation_retail['entity'],data_simulation_retail['material type']))
	data_predict['period'] = list(map(lambda x: str(x),data_predict['period']))
	data_predict['province_name'] = list(map(lambda x: "DIY" if x=="DI YOGYAKARTA" else x,data_predict['province_name']))
	data_model = pd.merge(
		data_simulation_retail,
		data_predict,
		left_on=['period', 'year','province','brand_name','packaging weight', 'district desc smi'],
		right_on=['period', 'year', 'province_name','brand_name','kemasan', 'district_name']
	)
	data_model = data_model.groupby(['period', 'year', 'province_name','brand_name','kemasan', 'district_name']).first().reset_index()
	data_model['prediction_price'] = data_model['prediction_price_x']
	data_model['prediction_volume'] = data_model['prediction_volume']
	data_model['volume'] = data_model['prediction_volume']
	data_model['volume_lm'] = data_model['volume_lm_x']
	data_model['htd_lm'] = data_model['htd_lm_x']
	data_model['volume_rkap'] = data_model['volume_rkap_x']
	data_model['rbp_rkap'] = data_model['rbp_rkap_x']
	data_model['revenue_rkap'] = data_model['revenue_rkap_x']
	data_model['district_ret'] = data_model['district_ret_x']
	data_model['brand_name_2'] = data_model['brand_name_2_x']
	data_model['oa_lm'] = data_model['oa_lm_x']
	data_model=data_model[['period', 'province', 'ship to code', 'ship to name', 'district_ret',
		'material type', 'packaging mode', 'year','month','packaging weight', 'entity',
		'region smi', 'district desc smi', 'company code/opco', 'brand_name','brand_name_2',
		'productive plant', 'shipping station l1 desc', 'incoterm', 'oa','oa_lm',
		'var prod', 'trn', 'kmsn', 'var packer', 'fix packer', 'fix prod',
		'adum', 'sales', 'com', 'biaya lain', 'oa ke customer', 'opt',
		'freight n container', 'freight', 'opp', 'oa to pelabuhan',
		'biaya social', 'gross margin distributor', 'margin distributor', 'wh allowance bpdd', 'pph', 'ppn',
		'channel_trx', 'net margin ics', 'revenue', 'penj net', 'cont margin',
		'gross margin', 'net margin', 'net margin grp', 'profit', 'last_price',
		'harga jual sub dist', 'harga reguler zak', 'harga tebus incl tax',
		'harga tebus excl tax', 'opco md excl tax', 'opco md netto excl tax',
		'termofpayment', 'model', 'segment','prediction_price','prediction_volume',
		'htd_lm','volume_rkap','rbp_rkap','revenue_rkap',
		'simulation_status', 'flag_change', 'rbp_lm', 'volume', 
		'disparitas_rbp_nbc_lm', 'volume_lm', 'rbp_nbc_lm', 'ms_nbc_lm', 
		'growth_rbp_3month', 'growth_rbp_6month', 'province_name_kfold_target_enc',
		'kemasan_kfold_target_enc','gpm','date_run','date_approve','simulation_id']]
	return data_model


def prep_cbp_modelling_b2b(data_simulation, data_predict):
	data_simulation_b2b = get_flag_change(data_simulation,'Price')

	data_model = pd.merge(
		data_simulation_b2b,
		data_predict,
		right_on=['period', 'year', 'province', 'ship_to_code', 'material_type'],
		left_on=['period', 'year', 'province', 'ship to code', 'material type']
	)

	data_model['prediction_price'] = data_model['prediction_price_x']
	data_model['prediction_volume'] = data_model['prediction_volume_x']
	data_model['ship to name'] = data_model['ship_to_name']
	data_model['ship to code'] = data_model['ship_to_code']
	data_model['material type'] = data_model['material_type']
	data_model['last_price'] = data_model['last_price_y']

	data_model=data_model[['period', 'year', 'month', 'province', 'brand_name_2', 'brand_name', 
	'ship to code', 'ship to name', 'material type', 'htd_lm', 'volume_lm', 'volume_rkap', 
	'rbp_rkap', 'revenue_rkap', 'packaging mode', 'packaging weight', 'brand_name', 'district_ret', 
	'entity', 'region smi', 'district desc smi', 'gpm', 'company code/opco', 'productive plant', 
	'shipping station l1 desc', 'incoterm', 'oa', 'oa_lm', 'var prod', 'trn', 'kmsn', 'var packer', 
	'fix packer', 'fix prod', 'adum', 'sales', 'com', 'biaya lain', 'oa ke customer', 'opt', 
	'freight n container', 'freight', 'opp', 'oa to pelabuhan', 'segment', 'biaya social', 
	'gross margin distributor', 'margin distributor', 'wh allowance bpdd', 'pph', 'ppn', 'channel_trx',
	'net margin ics', 'revenue', 'penj net', 'cont margin', 'gross margin', 'net margin', 'net margin grp',
	'profit', 'last_price', 'harga jual sub dist', 'harga reguler zak', 'harga tebus incl tax', 
	'harga tebus excl tax', 'opco md excl tax', 'opco md netto excl tax',
	'termofpayment', 'model', 'prediction_price', 'prediction_volume', 'simulation_status', 'flag_change',
	'date_run', 'date_approve', 'simulation_id']]

	return data_model


def calculate_gap_cbp(price,last_price):
	return price-last_price


def prep_vol_modelling_b2b(data_simulation, data_predict):
	data_model = get_flag_change(data_simulation,'Volume')
	# data_predict = data_predict.groupby(['period','ship_to_name','ship_to_code','province','district','group_pelanggan','material_type']).first().reset_index()
	data_model = pd.merge(
		data_simulation,
		data_predict,
		right_on=['period', 'province', 'ship_to_code', 'material_type'],
		left_on=['period', 'province', 'ship to code', 'material type']
	)
	data_res_model = pd.DataFrame()
	if len(data_model)>0:
		for material in data_model['material_type'].unique():
			data_mat = data_model[data_model['material_type']==material]
			material_type = material.lower().replace(' ','_')
			data_mat['gap_cbp_{0!s}'.format(material_type)] = list(map(lambda x,y:calculate_gap_cbp(x,y),data_mat['prediction_price'],data_mat['last_price']))
			data_res_model = data_res_model.append(data_mat)
		return data_res_model
	else:
		return data_model


def prep_vol_modelling_retail(data_simulation, data_predict):
	data_simulation_retail = get_flag_change(data_simulation,'Volume')
	data_predict['period'] = list(map(lambda x: str(x),data_predict['period']))
	data_predict['province_name'] = list(map(lambda x: "DIY" if x=="DI YOGYAKARTA" else x,data_predict['province_name']))
	# data_simulation_retail['brand_name'] = list(map(lambda x,y: grouping_entity(x,y),data_simulation_retail['entity'],data_simulation_retail['material type']))
	# data_predict = data_predict.groupby(['period', 'district_name', 'province_name', 'district_ret','brand_name', 'kemasan']).first().reset_index()
	# print(data_predict[['period', 'province_name','brand_name','packaging weight', 'district desc smi']].drop_duplicates())
	print('FOCUS')
	print(len(data_simulation_retail))
	print(data_simulation_retail[['period', 'province','brand_name','packaging weight', 'district desc smi']])

	print(len(data_predict))
	print(data_predict[['period', 'province_name','brand_name','kemasan', 'district_name']])


	data_model = pd.merge(
		data_simulation_retail,
		data_predict,
		left_on=['period', 'province','brand_name','packaging weight', 'district desc smi'],
		right_on=['period', 'province_name','brand_name','kemasan', 'district_name'],
	)
	data_model['rbp']=data_model['prediction_price']
	return data_model
	# data_res_model = pd.DataFrame()
	# if len(data_model)>0:
	# 	data_res_model['rbp']=data_res_model['prediction_price']
	# 	print('data Retail model volume')
	# 	print(len(data_res_model))
	# 	return data_res_model
	# else:
		# return data_model


def loop_apply_model_retail(brand_name,data_model,var_x,model_elasticity=model_elasticity_retail):
	data_model_brand = data_model[data_model['brand_name']==brand_name]
	if len(data_model_brand)>0:
		algorithm = model_elasticity[brand_name]
		file = open(F"./Modules/data/{algorithm}",'rb')

		var_x = np.array(var_x)
		var_takeout = "gap_rbp_{}".format(brand_name.lower())
		idx = np.where(var_x == var_takeout)
		var_x = np.delete(var_x, idx)

		volume_model = pickle.load(file)
		pred_vol = volume_model.predict(data_model_brand[var_x])
		data_model_brand['prediction_volume'] = pred_vol
		# data_model_brand['prediction_volume'] = list(map(lambda x,y : x+((max_a-y)/(max_a-min_a)*mean_a),data_model_brand['volume_lm'],pred_vol))
		data_model_brand['prediction_volume'] = list(map(lambda x : 0 if x < 0 else x,data_model_brand['prediction_volume']))
		return data_model_brand
	else:
		pass


def loop_apply_model_price_retail(brand_name,data_model,var_x,model_price=model_pricing_retail):
	data_model_brand = data_model[data_model['brand_name']==brand_name]
	if len(data_model_brand)>0:
		algorithm = model_price[brand_name]
		file = open(F"./Modules/data/{algorithm}",'rb')
		model = pickle.load(file)
		prediksi = model.predict(data_model_brand[var_x])
		data_model_brand['prediction_price'] = prediksi
		return data_model_brand
	else:
		pass


def loop_apply_model_b2b(material,data_model,var_x,model_elasticity=model_elasticity_b2b):
	data_model_material = data_model[data_model['material_type']==material]
	algorithm = model_elasticity[material]
	file = open(F"./Modules/data/{algorithm}",'rb')
	volume_model = pickle.load(file)
	pred_vol = volume_model.predict(data_model_material[var_x])
	max_a = data_model_brand['volume_lm'].max()
	min_a = data_model_brand['volume_lm'].min()
	mean_a = data_model_brand['volume_lm'].mean()
	data_model_material['prediction_volume'] = list(map(lambda x,y : x+((max_a-y)/(max_a-min_a)*mean_a),data_model_brand['volume_lm'],pred_vol))
	data_model_material['prediction_volume'] = list(map(lambda x : 0 if x < 0 else x,data_model_material['prediction_volume']))
	return data_model_material


def apply_model_b2b(data_model, flag):
	if flag == 'Price':
		file = open("./Modules/data/rbp_model_b2b.pkl",'rb')
		price_model = pickle.load(file)
		file.close()
		var_x = [
		'plant_to_distance_sig_nbc','last_sum_vol_6m','province_kfold_target_enc',
		'last_sow_sig_prov','last_gap_volume_to_nbc_prov','material_type_kfold_target_enc',
		'last_gap_sow_to_nbc_prov','last_sow_nbc_prov','last_freq_vol_6m',
		'last_gap_cbp_to_nbc_prov','last_sum_vol_1y','avg_vol_3m',
		'cbp_sig_prov','avg_vol_6m','prediction_volume',
		'last_freq_vol_1y','volume_sig_prov',
		'plant_to_distance_sig','cbp_nbc_prov','volume_nbc_prov',
		'last_sum_vol_2y','last_freq_vol_2y'
		]
		data_model['prediction_price'] = price_model.predict(data_model[var_x])
		return data_model
	elif flag == 'Volume':
		var_x = ['gap_cbp_pcc',
		   'gap_cbp_opc_reguler', 'gap_cbp_opc_premium', 'gap_cbp_owc',
		   'gap_cbp_putih', 'gap_cbp_pcc_premium', 'gap_cbp_type_v', 'gap_cbp_sbc',
		   'gap_cbp_type_ii', 'gap_cbp_duramax', 'gap_cbp_maxstrength',
		   'gap_cbp_ppc']
		data_res = pd.DataFrame()
		for material in data_model['material_type'].unique():
			data_model_material = loop_apply_model_b2b(material,data_model,var_x)
			data_res = data_res.append(data_model_material)
		return data_res


def apply_rbp(price_lm,volume1,volume2):
	price = price_lm + (-0.371789*(volume2-volume1))
	return price


def apply_model_retail(data_model, flag):
	if flag == 'Price':
		var_x = ['rbp_lm', 'volume', 'disparitas_rbp_nbc_lm', 'volume_lm', 'rbp_nbc_lm', 'ms_nbc_lm', 
		'growth_rbp_3month', 'growth_rbp_6month', 'province_name_kfold_target_enc', 'kemasan_kfold_target_enc']
		data_res = pd.DataFrame()
		for brand in data_model['brand_name'].unique():
			data_model_brand = loop_apply_model_price_retail(brand,data_model,var_x)
			data_res = data_res.append(data_model_brand)
		return data_res
	elif flag == 'Volume':
		data_model['volume_lm'] = data_model['volume_lm_x']
		data_model['district_ret'] = data_model['district_ret_x']
		var_x = ["volume_lm",'rbp','ms_lm','disparitas_rbp_lm','ms_nbc_lm',
		'gap_disparitas_rbp','gap_rbp_andalas', 'gap_rbp_dynamix', 'gap_rbp_sp', 'gap_rbp_powermax',
		'gap_rbp_sg', 'gap_rbp_masonry','gap_rbp_st', 'province_name_kfold_target_enc','kemasan_kfold_target_enc']
		data_res = pd.DataFrame()
		for brand in data_model['brand_name'].unique():
			data_model_brand = loop_apply_model_retail(brand,data_model,var_x)
			data_res = data_res.append(data_model_brand)
		return data_res


def get_cost_data(tabel_cost):
	statement = """
		select * from {0!s}
	""".format(tabel_cost)
	tabel_cost = pd.read_sql_query(statement, engine)
	return tabel_cost


def HargaTebusExcTax(district, entity,channel_trx,harga_tebus_incl_tax):
	if 'batam' in district.lower():
		return harga_tebus_incl_tax
	else:
		if entity != "SI":
			return harga_tebus_incl_tax/1.1
		elif entity == "SI":
			if channel_trx == "Distributor":
				return harga_tebus_incl_tax/1.1025
			elif channel_trx == "Direct":
				return harga_tebus_incl_tax/1.1
		else:
			return 0

	
def pph(entity, channel_trx, harga_tebus_excl_tax):
	if entity != "SI":
		return 0
	elif entity == "SI":
		if channel_trx == "Distributor":
			return harga_tebus_excl_tax*0.25/100
		else:
			return 0
	else:
		return 0

	
def OpcoMDExTax(entity,harga_tebus_exc_tax,var_prod,var_packer,kmsn,fix_prod,fix_packer,trn,oa,material,packaging_mode):
	if entity == "SBI":
		return harga_tebus_exc_tax*0.99
	elif entity == "SI":
		return 0
	elif entity == "SG":
		return var_prod+var_packer+kmsn+fix_prod+(fix_packer*(1+(14.74/100)))+trn+oa
	elif entity not in ['SBI','SI','SG']:
		if material in ['PCC','OPC','PCC PREMIUM']:
			if packaging_mode == "Small Bag":
				return harga_tebus_exc_tax-(harga_tebus_exc_tax*5/100)
			else:
				return var_prod+var_packer+kmsn+fix_prod+(fix_packer*(1+(14.74/100)))+trn+oa
		else:
			return var_prod+var_packer+kmsn+fix_prod+(fix_packer*(1+(14.74/100)))+trn+oa
	else:
		return var_prod+var_packer+kmsn+fix_prod+(fix_packer*(1+(14.74/100)))+trn+oa

	
def OpcoMDNettoExTax(entity,opco_md_ex_tax,oa,com,biaya_lain):
	if entity=="SI":
		return 0
	else:
		return opco_md_ex_tax-oa-com-biaya_lain


def ppn(district, harga_tebus):
	if 'batam' in district.lower():
		return 0
	else:
		return harga_tebus*0.1


def calculate_cost(data_simulation, data_cost, retail_distrik_, retail_province_):
	data_simulation_b2b = data_simulation[data_simulation['model']=='B2B']
	data_simulation_cost_b2b = pd.merge(
		data_simulation_b2b,
		data_cost, 
		left_on=[
			'province', 'ship to name', 'material type', 'entity',
			'productive plant','shipping station l1 desc','packaging mode',
			'packaging weight','incoterm','region smi','district desc smi'
		], 
		right_on=[
			'province', 'ship to name', 'material type','entity',
			'productive plant','shipping station l1 desc','packaging mode',
			'packaging weight','incoterm','region smi','district desc smi'
		]
	)

	data_simulation_cost_b2b['oa'] = data_simulation_cost_b2b['oa_x']
	data_simulation_cost_b2b['var prod'] = data_simulation_cost_b2b['var prod_x']
	data_simulation_cost_b2b['trn'] = data_simulation_cost_b2b['trn_x']
	data_simulation_cost_b2b['kmsn'] = data_simulation_cost_b2b['kmsn_x']
	data_simulation_cost_b2b['var packer'] = data_simulation_cost_b2b['var packer_x']
	data_simulation_cost_b2b['fix packer'] = data_simulation_cost_b2b['fix packer_x']
	data_simulation_cost_b2b['fix prod'] = data_simulation_cost_b2b['fix prod_x']
	data_simulation_cost_b2b['adum'] = data_simulation_cost_b2b['adum_x']
	data_simulation_cost_b2b['sales'] = data_simulation_cost_b2b['sales_x']
	data_simulation_cost_b2b['company code/opco'] = data_simulation_cost_b2b['company code/opco_x']
	data_simulation_cost_b2b['segment'] = data_simulation_cost_b2b['segment_x']

	data_simulation_cost_b2b['gross margin distributor'] = data_simulation_cost_b2b['gpm']/100*data_simulation_cost_b2b['prediction_price']
	data_simulation_cost_b2b['htd_inc_tax_ton'] = data_simulation_cost_b2b['prediction_price']-data_simulation_cost_b2b['gross margin distributor']
	data_simulation_cost_b2b['revenue'] = data_simulation_cost_b2b['htd_inc_tax_ton']*data_simulation_cost_b2b['prediction_volume']
	data_simulation_cost_b2b['penj net'] = data_simulation_cost_b2b['htd_inc_tax_ton']-data_simulation_cost_b2b['oa']
	data_simulation_cost_b2b['cont margin'] = data_simulation_cost_b2b['penj net']-(data_simulation_cost_b2b['var prod']+data_simulation_cost_b2b['trn']+data_simulation_cost_b2b['kmsn']+data_simulation_cost_b2b['var packer'])
	data_simulation_cost_b2b['gross margin'] = data_simulation_cost_b2b['cont margin']-(data_simulation_cost_b2b['fix packer']+data_simulation_cost_b2b['fix prod'])
	data_simulation_cost_b2b['net margin'] = data_simulation_cost_b2b['gross margin']-data_simulation_cost_b2b['adum']-data_simulation_cost_b2b['sales']
	data_simulation_cost_b2b['net margin grp'] = data_simulation_cost_b2b['net margin']+data_simulation_cost_b2b['net margin ics']
	data_simulation_cost_b2b['profit'] = data_simulation_cost_b2b['net margin grp']*data_simulation_cost_b2b['prediction_volume']
	
	data_simulation_cost_b2b['harga jual sub dist zak'] = data_simulation_cost_b2b['prediction_price']
	data_simulation_cost_b2b['harga reguler zak'] = data_simulation_cost_b2b['prediction_price'] + data_simulation_cost_b2b['wh allowance bpdd']
	data_simulation_cost_b2b['harga jual sub dist'] = data_simulation_cost_b2b['prediction_price']
	data_simulation_cost_b2b['margin distributor'] = data_simulation_cost_b2b['gross margin distributor']-data_simulation_cost_b2b['oa ke customer']-data_simulation_cost_b2b['opt']-data_simulation_cost_b2b['freight n container']-data_simulation_cost_b2b['freight']-data_simulation_cost_b2b['opp']-data_simulation_cost_b2b['oa to pelabuhan']-data_simulation_cost_b2b['biaya social']-data_simulation_cost_b2b['com']
	data_simulation_cost_b2b['harga tebus incl tax'] = data_simulation_cost_b2b['htd_inc_tax_ton']
	data_simulation_cost_b2b['harga tebus excl tax'] = list(map(lambda a,b,c,d: HargaTebusExcTax(a,b,c,d),data_simulation_cost_b2b['district desc smi'],data_simulation_cost_b2b['entity'],data_simulation_cost_b2b['channel_trx'],data_simulation_cost_b2b['harga tebus incl tax']))
	data_simulation_cost_b2b['pph'] = list(map(lambda a,b,c: pph(a,b,c),data_simulation_cost_b2b['entity'], data_simulation_cost_b2b['channel_trx'], data_simulation_cost_b2b['harga tebus excl tax']))																					   
	data_simulation_cost_b2b['ppn']=list(map(lambda x,y: ppn(x,y),data_simulation_cost_b2b['district desc smi'], data_simulation_cost_b2b['harga tebus excl tax']))
	data_simulation_cost_b2b['opco md excl tax'] = list(map(lambda a,b,c,d,e,f,g,h,i,j,k:OpcoMDExTax(a,b,c,d,e,f,g,h,i,j,k),data_simulation_cost_b2b['entity'],data_simulation_cost_b2b['harga tebus excl tax'],data_simulation_cost_b2b['var prod'],data_simulation_cost_b2b['var packer'],data_simulation_cost_b2b['kmsn'],data_simulation_cost_b2b['fix prod'],data_simulation_cost_b2b['fix packer'],data_simulation_cost_b2b['trn'],data_simulation_cost_b2b['oa'],data_simulation_cost_b2b['material type'],data_simulation_cost_b2b['packaging mode']))
	data_simulation_cost_b2b['opco md netto excl tax'] = list(map(lambda a,b,c,d,e:OpcoMDNettoExTax(a,b,c,d,e),data_simulation_cost_b2b['entity'],data_simulation_cost_b2b['opco md excl tax'],data_simulation_cost_b2b['oa'],data_simulation_cost_b2b['com'],data_simulation_cost_b2b['biaya lain']))



	drop_column = [
	'region smi_y','district desc smi_y',
	'oa_y','var prod_y','trn_y',
	'kmsn_y','var packer_y',
	'fix packer_y','fix prod_y',
	'adum_y','sales_y','segment_y',
	'company code/opco_y','ship to code_y',
	'ship to name_y','material type_y',
	'region smi_x','district desc smi_x',
	'oa_x','var prod_x','trn_x',
	'kmsn_x','var packer_x',
	'fix packer_x','fix prod_x',
	'adum_x','sales_x','segment_x',
	'company code/opco_x','ship to code_x',
	'ship to name_x','material type_x', 'htd_inc_tax_ton',
	'htd_inc_tax','kemasan_','weight_kemasan','predict_med_new_y','predict_med_new'
	]
	data_simulation_cost_b2b_column = set(data_simulation_cost_b2b.columns)-set(drop_column)
	data_simulation_cost_b2b = data_simulation_cost_b2b[data_simulation_cost_b2b_column]


	data_simulation_retail = data_simulation[data_simulation['model']=='Retail']

	data_simulation_cost_retail = pd.merge(
		data_simulation_retail,
		data_cost, 
		on=[
			'province', 'entity', 'region smi','district desc smi',
			'productive plant','shipping station l1 desc','packaging mode',
			'packaging weight','incoterm'
		]
	)

	data_simulation_cost_retail['oa'] = data_simulation_cost_retail['oa_x']
	data_simulation_cost_retail['var prod'] = data_simulation_cost_retail['var prod_x']
	data_simulation_cost_retail['trn'] = data_simulation_cost_retail['trn_x']
	data_simulation_cost_retail['kmsn'] = data_simulation_cost_retail['kmsn_x']
	data_simulation_cost_retail['var packer'] = data_simulation_cost_retail['var packer_x']
	data_simulation_cost_retail['fix packer'] = data_simulation_cost_retail['fix packer_x']
	data_simulation_cost_retail['fix prod'] = data_simulation_cost_retail['fix prod_x']
	data_simulation_cost_retail['adum'] = data_simulation_cost_retail['adum_x']
	data_simulation_cost_retail['sales'] = data_simulation_cost_retail['sales_x']
	data_simulation_cost_retail['company code/opco'] = data_simulation_cost_retail['company code/opco_x']
	data_simulation_cost_retail['ship to name'] = data_simulation_cost_retail['ship to name_x']
	data_simulation_cost_retail['ship to code'] = data_simulation_cost_retail['ship to code_x']
	data_simulation_cost_retail['material type'] = data_simulation_cost_retail['material type_x']
	data_simulation_cost_retail['segment'] = data_simulation_cost_retail['segment_x']
	data_simulation_cost_retail['gross margin distributor zak'] = list(map(lambda x,y: x*y/100,data_simulation_cost_retail['prediction_price'],data_simulation_cost_retail['gpm']))
	data_simulation_cost_retail['htd_inc_tax'] = list(map(lambda x,y: int(x) if math.isnan(y) else int(x)-int(y),data_simulation_cost_retail['prediction_price'],data_simulation_cost_retail['gross margin distributor zak']))
	data_simulation_cost_retail['kemasan_'] = list(map(lambda x: int(x.split(' ')[0]),data_simulation_cost_retail['packaging weight']))
	data_simulation_cost_retail['weight_kemasan'] = 1000/data_simulation_cost_retail['kemasan_']
	data_simulation_cost_retail['htd_inc_tax'] = list(map(lambda x,y: int(x) if math.isnan(y) else int(x)-int(y),data_simulation_cost_retail['prediction_price'],data_simulation_cost_retail['gross margin distributor zak']))
	data_simulation_cost_retail['htd_exc_tax'] = list(map(lambda a,b,c,d: HargaTebusExcTax(a,b,c,d),data_simulation_cost_retail['district desc smi'],data_simulation_cost_retail['entity'],data_simulation_cost_retail['channel_trx'],data_simulation_cost_retail['htd_inc_tax']))
	data_simulation_cost_retail['htd_exc_tax_ton'] = data_simulation_cost_retail['htd_exc_tax']*data_simulation_cost_retail['weight_kemasan']
	data_simulation_cost_retail['htd_inc_tax_ton'] = data_simulation_cost_retail['htd_inc_tax']*data_simulation_cost_retail['weight_kemasan']

	data_simulation_cost_retail['revenue'] = data_simulation_cost_retail['htd_exc_tax_ton']*data_simulation_cost_retail['prediction_volume']
	data_simulation_cost_retail['penj net'] = data_simulation_cost_retail['htd_exc_tax_ton']-data_simulation_cost_retail['oa_lm']
	data_simulation_cost_retail['cont margin'] = data_simulation_cost_retail['penj net']-(data_simulation_cost_retail['var prod']+data_simulation_cost_retail['trn']+data_simulation_cost_retail['kmsn']+data_simulation_cost_retail['var packer'])
	data_simulation_cost_retail['gross margin'] = data_simulation_cost_retail['cont margin']-(data_simulation_cost_retail['fix packer']+data_simulation_cost_retail['fix prod'])
	data_simulation_cost_retail['net margin'] = data_simulation_cost_retail['gross margin']-data_simulation_cost_retail['adum']-data_simulation_cost_retail['sales']
	data_simulation_cost_retail['net margin grp'] = data_simulation_cost_retail['net margin']+data_simulation_cost_retail['net margin ics']
	data_simulation_cost_retail['profit'] = data_simulation_cost_retail['net margin grp']*data_simulation_cost_retail['prediction_volume']

	data_simulation_cost_retail['gross margin distributor zak'] = (data_simulation_cost_retail['prediction_price']*data_simulation_cost_retail['gpm']/100)
	data_simulation_cost_retail['gross margin distributor'] = data_simulation_cost_retail['gross margin distributor zak']*data_simulation_cost_retail['weight_kemasan']
	data_simulation_cost_retail['margin distributor zak'] = data_simulation_cost_retail['gross margin distributor zak']-data_simulation_cost_retail['oa ke customer']-data_simulation_cost_retail['opt']-data_simulation_cost_retail['freight n container']-data_simulation_cost_retail['freight']-data_simulation_cost_retail['opp']-data_simulation_cost_retail['oa to pelabuhan']-data_simulation_cost_retail['biaya social']-data_simulation_cost_retail['com']
	data_simulation_cost_retail['margin distributor'] = data_simulation_cost_retail['margin distributor zak']*data_simulation_cost_retail['weight_kemasan']
	data_simulation_cost_retail['harga jual sub dist zak'] = data_simulation_cost_retail['prediction_price']
	data_simulation_cost_retail['harga reguler zak'] = data_simulation_cost_retail['prediction_price'] + data_simulation_cost_retail['wh allowance bpdd']
	data_simulation_cost_retail['harga jual sub dist'] = data_simulation_cost_retail['prediction_price']*data_simulation_cost_retail['weight_kemasan'] 
	data_simulation_cost_retail['harga tebus incl tax'] = data_simulation_cost_retail['htd_inc_tax_ton']
	data_simulation_cost_retail['harga tebus excl tax'] = list(map(lambda a,b,c,d: HargaTebusExcTax(a,b,c,d),data_simulation_cost_retail['district desc smi'],data_simulation_cost_retail['entity'],data_simulation_cost_retail['channel_trx'],data_simulation_cost_retail['harga tebus incl tax']))
	data_simulation_cost_retail['pph'] = list(map(lambda a,b,c: pph(a,b,c),data_simulation_cost_retail['entity'], data_simulation_cost_retail['channel_trx'], data_simulation_cost_retail['harga tebus excl tax']))   
	data_simulation_cost_retail['ppn']=list(map(lambda x,y: ppn(x,y),data_simulation_cost_retail['district desc smi'], data_simulation_cost_retail['harga tebus excl tax']))
	data_simulation_cost_retail['opco md excl tax'] = list(map(lambda a,b,c,d,e,f,g,h,i,j,k:OpcoMDExTax(a,b,c,d,e,f,g,h,i,j,k),data_simulation_cost_retail['entity'],data_simulation_cost_retail['harga tebus excl tax'],data_simulation_cost_retail['var prod'],data_simulation_cost_retail['var packer'],data_simulation_cost_retail['kmsn'],data_simulation_cost_retail['fix prod'],data_simulation_cost_retail['fix packer'],data_simulation_cost_retail['trn'],data_simulation_cost_retail['oa'],data_simulation_cost_retail['material type'],data_simulation_cost_retail['packaging mode']))
	data_simulation_cost_retail['opco md netto excl tax'] = list(map(lambda a,b,c,d,e:OpcoMDNettoExTax(a,b,c,d,e),data_simulation_cost_retail['entity'],data_simulation_cost_retail['opco md excl tax'],data_simulation_cost_retail['oa'],data_simulation_cost_retail['com'],data_simulation_cost_retail['biaya lain']))
	df_predict_cost_filter_opt_first_join_distrik = pd.merge(data_simulation_cost_retail,retail_distrik_,on=['period','province','district_ret'],how='left')
	df_predict_cost_filter_opt_first_join_province = pd.merge(df_predict_cost_filter_opt_first_join_distrik,retail_province_,on=['period','province'])
	
	df_predict_cost_filter_opt_first_join_province['predict_med_new_y']=list(map(lambda x: 0 if x<=0 else x,df_predict_cost_filter_opt_first_join_province['predict_med_new_y']))
	df_predict_cost_filter_opt_first_join_province['predict_med_new']=list(map(lambda x: 0 if x<=0 else x,df_predict_cost_filter_opt_first_join_province['predict_med_new_x']))
	df_predict_cost_filter_opt_first_join_province['market_share'] = list(map(lambda v,x,y,z:0 if x<=0 else (x/y*100 if v!='UNKNOWN' else x/z*100),df_predict_cost_filter_opt_first_join_province['district_ret'],df_predict_cost_filter_opt_first_join_province['prediction_volume'],df_predict_cost_filter_opt_first_join_province['predict_med_new_y'],df_predict_cost_filter_opt_first_join_province['predict_med_new']))
	# data_simulation_cost_retail['market_share'] = df_predict_cost_filter_opt_first_join_province['market_share']
	data_simulation_cost_b2b['market_share'] = 0

	data_simulation_cost_retail_column = set(df_predict_cost_filter_opt_first_join_province.columns)-set(drop_column)
	data_simulation_cost_retail = df_predict_cost_filter_opt_first_join_province[data_simulation_cost_retail_column]
	

	data_simulation_cost=data_simulation_cost_retail.append(data_simulation_cost_b2b,ignore_index=True)
	data_simulation_cost = data_simulation_cost.groupby(['period', 'year', 'month', 'province', 'material type','packaging mode', 'packaging weight', 'brand_name','entity', 'region smi', 'district desc smi']).first().reset_index()




	return data_simulation_cost


def grouping_province(prov):
	if prov.lower() in ['jawa barat', 'jawa timur', 'banten','jawa tengah', 'bali', 'di yogyakarta', 'dki jakarta']:
		return 1
	else:
		return 0


def transform_encoded_new_cust(col,val):
	encoder_file = './Modules/data/b2b_new_cust_{0!s}_target_encoding.csv'.format(col)
	encoder_pd = pd.read_csv(encoder_file)
	data_filter = encoder_pd[encoder_pd[col]==val].reset_index()
	encoded_var = '{0!s}_kfold_target_enc'.format(col)
	return data_filter[encoded_var][0]


def get_mapping_district_var(engine,tabel_mapping_var='mapping_district_var_customer_baru_test'):
	statement = """
		select * from {0!s}
	""".format(tabel_mapping_var)
	data_mapping_district_var = pd.read_sql_query(statement, engine)
	return data_mapping_district_var



