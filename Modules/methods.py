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
	data_predict['period'] = list(map(lambda x: int(x),data_predict['period']))
	data_simulation_retail['period'] = list(map(lambda x: int(x),data_simulation_retail['period']))

	data_predict['year'] = list(map(lambda x: str(x)[:5],data_predict['period']))
	data_simulation_retail['year'] = list(map(lambda x: str(x)[:5],data_simulation_retail['period']))

	data_predict['province_name'] = list(map(lambda x: "DIY" if x=="DI YOGYAKARTA" else x,data_predict['province_name']))
	data_model = pd.merge(
		data_simulation_retail,
		data_predict,
		left_on=['period', 'year','province','brand_name','packaging weight', 'district desc smi'],
		right_on=['period', 'year', 'province_name','brand_name','kemasan', 'district_name']
	)
	data_model = data_model.groupby(['period', 'year', 'province_name','brand_name','kemasan', 'district_name']).first().reset_index()
	data_model['volume'] = data_model['prediction_volume']
	data_model['volume_lm'] = data_model['volume_lm_x']
	data_model['rbp_lm'] = data_model['rbp_lm_x']
	data_model['ms_lm'] = data_model['ms_lm_x']
	data_model['brand_name_2'] = data_model['brand_name_2_x']
	return data_model


def prep_cbp_modelling_b2b(data_simulation, data_predict):
	data_simulation_b2b = get_flag_change(data_simulation,'Price')

	data_simulation_b2b['ship to code'] = list(map(lambda x: str(x).replace('.0',''),data_simulation_b2b['ship to code']))
	data_predict['ship_to_code'] = list(map(lambda x: str(x).replace('.0',''),data_predict['ship_to_code']))

	print(len(data_simulation_b2b))

	data_simulation_b2b['period'] = list(map(lambda x: int(x),data_simulation_b2b['period']))
	data_predict['period'] = list(map(lambda x: int(x),data_predict['period']))

	print(len(data_simulation_b2b))

	data_model = pd.merge(
		data_simulation_b2b,
		data_predict,
		right_on=['period','province_name', 'material_type','district_name', 'ship_to_name', 'ship_to_code'],
		left_on=['period', 'province','material type','district desc smi','ship to name', 'ship to code']
	)

	print(len(data_model))

	data_model['volume'] = data_model['prediction_volume']
	data_model['delta_volume'] = data_model['volume']-data_model['volume_lm_y']
	data_model['volume_lm'] = data_model['volume_lm_y']
	# data_model['volume_rkap'] = data_model['volume_rkap_x']
	# data_model['revenue_rkap'] = data_model['revenue_rkap_x']
	# data_model['volume_rkap'] = data_model['volume_rkap_x']
	# data_model['revenue_rkap'] = data_model['revenue_rkap_x']
	data_model['prediction_price'] = data_model['prediction_price_x']
	data_model['termofpayment'] = data_model['termofpayment_y']


	return data_model


def calculate_gap_cbp(price,last_price):
	return price-last_price


def prep_vol_modelling_b2b(data_simulation, data_predict):
	data_simulation_b2b = get_flag_change(data_simulation,'Volume')

	data_simulation_b2b['ship to code'] = list(map(lambda x: str(x).replace('.0',''),data_simulation_b2b['ship to code']))
	data_predict['ship_to_code'] = list(map(lambda x: str(x).replace('.0',''),data_predict['ship_to_code']))

	data_simulation_b2b['period'] = list(map(lambda x: int(x),data_simulation_b2b['period']))
	data_predict['period'] = list(map(lambda x: int(x),data_predict['period']))

	data_model = pd.merge(
		data_simulation_b2b,
		data_predict,
		right_on=['period','province_name', 'material_type','district_name', 'ship_to_name', 'ship_to_code'],
		left_on=['period', 'province','material type','district desc smi','ship to name', 'ship to code']
	)
	data_model['cbp'] = data_model['prediction_price']
	data_model['delta_cbp'] = data_model['cbp']-data_model['cbp_lm']
	data_model['termofpayment'] = data_model['termofpayment_y']
	return data_model


	# data_res_model = pd.DataFrame()
	# if len(data_model)>0:
	# 	for material in data_model['material_type'].unique():
	# 		data_mat = data_model[data_model['material_type']==material]
	# 		material_type = material.lower().replace(' ','_')
	# 		data_mat['gap_cbp_{0!s}'.format(material_type)] = list(map(lambda x,y:calculate_gap_cbp(x,y),data_mat['prediction_price'],data_mat['last_price']))
	# 		data_res_model = data_res_model.append(data_mat)
	# 	return data_res_model
	# else:
	# 	return data_model


def prep_vol_modelling_retail(data_simulation, data_predict):
	data_simulation_retail = get_flag_change(data_simulation,'Volume')
	data_predict['period'] = list(map(lambda x: int(x),data_predict['period']))
	data_simulation_retail['period'] = list(map(lambda x: int(x),data_simulation_retail['period']))
	data_predict['province_name'] = list(map(lambda x: "DIY" if x=="DI YOGYAKARTA" else x,data_predict['province_name']))
	# data_simulation_retail['brand_name'] = list(map(lambda x,y: grouping_entity(x,y),data_simulation_retail['entity'],data_simulation_retail['material type']))
	# data_predict = data_predict.groupby(['period', 'district_name', 'province_name', 'district_ret','brand_name', 'kemasan']).first().reset_index()
	# print(data_predict[['period', 'province_name','brand_name','packaging weight', 'district desc smi']].drop_duplicates())


	data_model = pd.merge(
		data_simulation_retail,
		data_predict,
		left_on=['period', 'province','brand_name','packaging weight', 'district desc smi'],
		right_on=['period', 'province_name','brand_name','kemasan', 'district_name'],
	)
	# data_model['prediction_volume'] = data_model['prediction_volume_x']
	# data_model['predict_high'] = data_model['predict_high_x']
	# data_model['predict_low'] = data_model['predict_low_x']
	# data_model['predict_med'] = data_model['predict_med_x']
	data_model['rbp_lm'] = data_model['rbp_lm_x']
	data_model['ms_lm'] = data_model['ms_lm_x']
	data_model['volume_lm'] = data_model['volume_lm_x']
	data_model['rbp']=data_model['prediction_price']
	data_model['disparitas_rbp_nbc'] = data_model['rbp']-data_model['rbp_nbc_lm']
	data_model['disparitas_rbp_lm'] = data_model['rbp_lm']-data_model['rbp_nbc_lm']
	data_model['gap_disparity'] = data_model['disparitas_rbp_nbc']-data_model['disparitas_rbp_nbc_lm']
	return data_model
	# data_res_model = pd.DataFrame()
	# if len(data_model)>0:
	# 	data_res_model['rbp']=data_res_model['prediction_price']
	# 	print('data Retail model volume')
	# 	print(len(data_res_model))
	# 	return data_res_model
	# else:
		# return data_model


def loop_apply_model_retail(brand_name,data_model,var_x,model_volume=model_elasticity_retail):
	data_model_brand = data_model[data_model['brand_name']==brand_name]
	if len(data_model_brand)>0:
		algorithm = model_volume[brand_name]
		file = open(F"./Modules/data/{algorithm}",'rb')
		volume_model = pickle.load(file)
		print(data_model_brand.columns)
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


# def loop_apply_model_b2b(material,data_model,var_x,model_elasticity=model_elasticity_b2b):
# 	data_model_material = data_model[data_model['material_type']==material]
# 	algorithm = model_elasticity[material]
# 	file = open(F"./Modules/data/{algorithm}",'rb')
# 	volume_model = pickle.load(file)
# 	pred_vol = volume_model.predict(data_model_material[var_x])
# 	max_a = data_model_brand['volume_lm'].max()
# 	min_a = data_model_brand['volume_lm'].min()
# 	mean_a = data_model_brand['volume_lm'].mean()
# 	data_model_material['prediction_volume'] = list(map(lambda x,y : x+((max_a-y)/(max_a-min_a)*mean_a),data_model_brand['volume_lm'],pred_vol))
# 	data_model_material['prediction_volume'] = list(map(lambda x : 0 if x < 0 else x,data_model_material['prediction_volume']))
# 	return data_model_material


def apply_model_b2b(data_model, flag):
	if flag == 'Price':
		file = open("./Modules/data/model_randomforest_b2b_v2p_no_price(1).pkl",'rb')
		price_model = pickle.load(file)
		file.close()
		var_x = [
			'volume', 'cbp_nbc', 'is_seasonality', 'sow_lm', 'sow_l2m',
			'sow_nbc_lm', 'sow_nbc_l2m', 'volume_lm', 'volume_l2m',
			'disparitas_cbp_nbc_lm', 'disparitas_cbp_nbc_l2m', 'termofpayment',
			'plant_to_distance_sig', 'plant_to_distance_sig_nbc', 'delta_volume',
			'delta_volume_lm', 'material_type_encoded', 'province_name_encoded',
			'segmentsi_encoded'
		]
		data_model['prediction_price'] = price_model.predict(data_model[var_x])
		return data_model
	elif flag == 'Volume':
		file = open("./Modules/data/model_randomforest_b2b_p2v_no_volume(1).pkl",'rb')
		volume_model = pickle.load(file)
		var_x = ['cbp', 'cbp_nbc', 'cbp_lm', 'cbp_l2m', 'is_seasonality', 'sow_lm',
	       'sow_l2m', 'sow_nbc_lm', 'sow_nbc_l2m', 'disparitas_cbp_nbc_lm',
	       'disparitas_cbp_nbc_l2m', 'termofpayment', 'plant_to_distance_sig',
	       'plant_to_distance_sig_nbc', 'delta_cbp', 'delta_cbp_lm',
	       'material_type_encoded', 'province_name_encoded', 'segmentsi_encoded',
	       'district_name_encoded']
		data_model['prediction_volume'] = volume_model.predict(data_model[var_x])
		return data_model


def apply_model_retail(data_model, flag):
	if flag == 'Price':
		var_x = ['gap_rbp_sp_lm', 'ms_l2m', 'ms_lm', 'gap_disparity_lm', 'rbp_nbc_lm',
			'gap_rbp_masonry_lm', 'rbp_l2m',
			'gap_rbp_dynamix_lm', 'rbp_lm',
			'gpm_zak_lm', 'is_seasonality', 'ms_nbc_l2m', 'volume',
			'volume_l2m', 'gap_rbp_st_lm', 'rbp_nbc_l2m',
			'gap_rbp_si_lm', 'stock_level',
			'volume_lm', 'disparitas_rbp_nbc_l2m',
			'gap_rbp_sg_lm', 'ms_nbc_lm', 'gap_rbp_powermax_lm', 
			 "province_name_kfold_target_enc","kemasan_kfold_target_enc",
			'gap_rbp_andalas_lm', 'disparitas_rbp_nbc_lm', 'gpm_zak_l2m','prediction_demand']
		data_res = pd.DataFrame()
		for brand in data_model['brand_name'].unique():
			data_model_brand = loop_apply_model_price_retail(brand,data_model,var_x)
			data_res = data_res.append(data_model_brand)
		return data_res
	elif flag == 'Volume':
		var_x = ['gap_rbp_sp_lm', 'ms_l2m', 'ms_lm', 'gap_disparity_lm', 'rbp_nbc_lm',
				'gap_rbp_masonry_lm', 'rbp_l2m', 'rbp_nbc',
				'gap_rbp_dynamix_lm', 'rbp_lm', 'gap_disparity',
				'gpm_zak_lm', 'is_seasonality', 'ms_nbc_l2m', 'rbp',
				'volume_l2m', 'gap_rbp_st_lm', 'rbp_nbc_l2m',
				'gap_rbp_si_lm', 'stock_level',
				'volume_lm', 'disparitas_rbp_nbc_l2m',
				'gap_rbp_sg_lm', 'ms_nbc_lm', 'gap_rbp_powermax_lm', 
				 "province_name_kfold_target_enc","kemasan_kfold_target_enc",
				'gap_rbp_andalas_lm', 'disparitas_rbp_nbc_lm', 'gpm_zak_l2m','prediction_demand']
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
			return harga_tebus_incl_tax/1.11
		elif entity == "SI":
			if channel_trx == "Distributor":
				return harga_tebus_incl_tax/1.1125
			elif channel_trx == "Direct":
				return harga_tebus_incl_tax/1.11
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
	data_simulation_cost = pd.DataFrame()

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
	'ship to name_x','material type_x',
	'htd_inc_tax','kemasan_','predict_med_new_y','predict_med_new'
	]

	data_simulation_b2b = data_simulation[data_simulation['model']=='B2B']
	if len(data_simulation_b2b)>0:
		data_cost = data_cost[data_cost['segment']=='CorSales']
		data_simulation_b2b['ship to code'] = list(map(lambda x: str(x).replace('.0',''),data_simulation_b2b['ship to code']))
		data_cost['ship to code'] = list(map(lambda x: str(x).replace('.0',''),data_cost['ship to code']))
		
		x = data_simulation_b2b['ship to code'].unique()
		y = data_cost['ship to code'].unique()

		data_simulation_cost_b2b = pd.merge(
			data_simulation_b2b,
			data_cost, 
			on=[
				'province', 'ship to code','ship to name', 'material type',
				'productive plant','shipping station l1 desc',
				'incoterm','district desc smi'
			],
			how='left'
		)

		filler = {
	        'segment_x': 'CorSales',
	        'entity_x':'UNKNOWN',
	        'company code/opco_x': 'UNKNOWN',
	        'productive plant': 'UNKNOWN',
	        'shipping station l1 desc': 'UNKNOWN',
	        'packaging mode_x': 'Bulk',
	        'region smi_x':'UNKNOWN',
	        'incoterm_x':'UNKNOWN',
	        'ship to code_x':'UNKNOWN',
	        'ship to name_x':'UNKNOWN',
	        'material type_x': 'UNKNOWN',
	        'province_x':'UNKNOWN',
	        'oa_x':0, 
	        'var prod_x':0, 
	        'trn_x':0, 
	        'kmsn_x':0,
	        'var packer_x':0, 
	        'fix packer_x':0, 
	        'fix prod_x':0, 
	        'adum_x':0, 
	        'sales_x':0
	    }
		
		data_simulation_cost_b2b = data_simulation_cost_b2b.fillna(filler)

		data_simulation_cost_b2b['entity'] = data_simulation_cost_b2b['entity_x']
		data_simulation_cost_b2b['packaging mode'] = data_simulation_cost_b2b['packaging mode_x']
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
		data_simulation_cost_b2b['packaging weight'] = data_simulation_cost_b2b['packaging weight_x']
		data_simulation_cost_b2b['region smi'] = data_simulation_cost_b2b['region smi_x']


		data_simulation_cost_b2b['gross margin distributor'] = data_simulation_cost_b2b['margin distributor']+data_simulation_cost_b2b['oa ke customer']+data_simulation_cost_b2b['opt']+data_simulation_cost_b2b['freight n container']+data_simulation_cost_b2b['freight']+data_simulation_cost_b2b['opp']+data_simulation_cost_b2b['oa to pelabuhan']+data_simulation_cost_b2b['biaya social']+data_simulation_cost_b2b['com']
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
		data_simulation_cost_b2b_column = set(data_simulation_cost_b2b.columns)-set(drop_column)
		data_simulation_cost_b2b = data_simulation_cost_b2b[data_simulation_cost_b2b_column]
		data_simulation_cost_b2b['market_share'] = 0
		data_simulation_cost_b2b = data_simulation_cost_b2b.groupby([
			'period', 'ship to code','ship to name', 'material type',
			'district desc smi'
		]).first().reset_index()

		data_simulation_cost = data_simulation_cost.append(data_simulation_cost_b2b,ignore_index=True)

	data_simulation_retail = data_simulation[data_simulation['model']=='Retail']
	if len(data_simulation_retail)>0:

		data_simulation_cost_retail = pd.merge(
			data_simulation_retail,
			data_cost, 
			on=[
				'province', 'entity', 'region smi','district desc smi',
				'productive plant','shipping station l1 desc','packaging mode',
				'packaging weight','incoterm'
			]
		)

		data_simulation_cost_retail['oa'] = data_simulation_cost_retail['oa_lm']
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
		# df_predict_cost_filter_opt_first_join_distrik = pd.merge(data_simulation_cost_retail,retail_distrik_,on=['period','province','district_ret'],how='left')
		# df_predict_cost_filter_opt_first_join_province = pd.merge(df_predict_cost_filter_opt_first_join_distrik,retail_province_,on=['period','province'],how='left')
		df_predict_cost_filter_opt_first_join_province = data_simulation_cost_retail
		df_predict_cost_filter_opt_first_join_province['market_share'] = 0
		# df_predict_cost_filter_opt_first_join_province['predict_med_new_y']=list(map(lambda x: 0 if x<=0 else x,df_predict_cost_filter_opt_first_join_province['predict_med_new_y']))
		# df_predict_cost_filter_opt_first_join_province['predict_med_new']=list(map(lambda x: 0 if x<=0 else x,df_predict_cost_filter_opt_first_join_province['predict_med_new_x']))
		# df_predict_cost_filter_opt_first_join_province['market_share'] = list(map(lambda v,x,y,z:0 if x<=0 else (x/y*100 if v!='UNKNOWN' else x/z*100),df_predict_cost_filter_opt_first_join_province['district_ret'],df_predict_cost_filter_opt_first_join_province['prediction_volume'],df_predict_cost_filter_opt_first_join_province['predict_med_new_y'],df_predict_cost_filter_opt_first_join_province['predict_med_new']))
		# data_simulation_cost_retail['market_share'] = df_predict_cost_filter_opt_first_join_province['market_share']

		data_simulation_cost_retail_column = set(df_predict_cost_filter_opt_first_join_province.columns)-set(drop_column)
		data_simulation_cost_retail = df_predict_cost_filter_opt_first_join_province[data_simulation_cost_retail_column]
		data_simulation_cost_retail = data_simulation_cost_retail.groupby(['period', 'year', 'month', 'province', 'material type','packaging mode', 'packaging weight', 'brand_name','entity', 'region smi', 'district desc smi']).first().reset_index()

		data_simulation_cost = data_simulation_cost.append(data_simulation_cost_retail,ignore_index=True)
	
	return data_simulation_cost


def flagging_gain_loss(x):
		if x<-20:
			return 1
		elif x<-10:
			return 2
		elif x<-5:
			return 3
		elif x<-2:
			return 4
		elif x<0:
			return 5
		elif x<2:
			return 6
		elif x<5:
			return 7
		elif x<10:
			return 8
		elif x<20:
			return 9
		else:
			return 10


def cek_makesense(data,path_mapping = path_data_mapping_bisnis):
	data['gain_loss_profit'] = data['profit'] - data['profit_if_not_change']
	data['gain_loss_demand'] = data['prediction_volume'] - data['demand_if_not_change']
	data['gain_loss_revenue'] = data['revenue'] - data['revenue_if_not_change']
	data['demand_lm'] = list(
		map(
			lambda x,y: x/(y/100) if y>0 else 0,
			data['volume_lm'],
			data['ms_lm']
		)
	)
	data['gain_drop_ms'] = list(
		map(
			lambda a,b,c : ((a/b)*100)-c if b>0 else 0, 
			data['prediction_volume'],
			data['demand_lm'],
			data['ms_lm']
		)
	)
	data['gain_drop_rbp'] = list(
		map(
			lambda a,b : ((b-a)/a)*100 if a>0 else 0, 
			data['rbp_lm'],
			data['prediction_price']
		)
	)
	data['flag_ms'] = list(map(lambda x: flagging_gain_loss(x),data['gain_drop_ms']))
	data['flag_rbp'] = list(map(lambda x: flagging_gain_loss(x),data['gain_drop_rbp']))
	data_mapping_business = pd.read_csv(path_data_mapping_bisnis)
	data = pd.merge(data,data_mapping_business,on=['flag_rbp','flag_ms'])
	data['keterangan_makesense'] = data['keterangan_makesense_y']
	data['is_makesense'] = data['is_makesense_y']
	print(data[['keterangan_makesense_y','keterangan_makesense_x']])
	drop_column = [
	"keterangan_makesense_y","is_makesense_y","keterangan_makesense_x","is_makesense_x"
	]
	data_col = list(set(data.columns)-set(drop_column))
	data = data[data_col]
	return data



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





