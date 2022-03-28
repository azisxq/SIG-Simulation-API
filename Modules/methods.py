from sqlalchemy import create_engine
import pandas as pd
import pickle
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV

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

    data_simulation_retail['brand_name'] = list(map(lambda x,y: grouping_entity(x,y),data_simulation_retail['entity'],data_simulation_retail['material type']))
    data_model = pd.merge(
        data_simulation_retail,
        data_predict,
        left_on=['period', 'year','province','brand_name','packaging weight', 'district desc smi'],
        right_on=['period', 'year', 'province_name','brand_name','kemasan', 'district_name']
    )

    data_model['prediction_price'] = data_model['prediction_price_x']
    data_model['prediction_volume'] = data_model['predict_med_new']
    data_model=data_model[['period', 'province', 'ship to code', 'ship to name',
       'material type', 'packaging mode', 'year','month','packaging weight', 'entity',
       'region smi', 'district desc smi', 'company code/opco',
       'productive plant', 'shipping station l1 desc', 'incoterm', 'oa',
       'var prod', 'trn', 'kmsn', 'var packer', 'fix packer', 'fix prod',
       'adum', 'sales', 'com', 'biaya lain', 'oa ke customer', 'opt',
       'freight n container', 'freight', 'opp', 'oa to pelabuhan',
       'biaya social', 'margin distributor', 'wh allowance bpdd', 'pph', 'ppn',
       'channel_trx', 'net margin ics', 'revenue', 'penj net', 'cont margin',
       'gross margin', 'net margin', 'net margin grp', 'profit', 'last_price',
       'harga jual sub dist', 'harga reguler', 'harga tebus incl tax',
       'harga tebus excl tax', 'opco md excl tax', 'opco md netto excl tax',
       'termofpayment', 'model',
       'simulation_status', 'flag_change', 'is_seasonality', 'is_promo', 'ms_brand', 
       'volume_brand', 'rbp_lm_nbc',
	   'stok_level', 'growth_rbp_3month', 'growth_rbp_6month','growth_volume_1month', 'kemasan_kfold_target_enc',
	   'brand_name_kfold_target_enc', 'prediction_price','predict_med_new',
       'prediction_volume','gpm','simulation_id']]
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
    data_model=data_model[['period', 'year','month', 'province', 'ship to code', 'ship to name',
       'material type', 'packaging mode', 'packaging weight', 'entity',
       'region smi', 'district desc smi', 'gpm', 'company code/opco',
       'productive plant', 'shipping station l1 desc', 'incoterm', 'oa',
       'var prod', 'trn', 'kmsn', 'var packer', 'fix packer', 'fix prod',
       'adum', 'sales', 'com', 'biaya lain', 'oa ke customer', 'opt',
       'freight n container', 'freight', 'opp', 'oa to pelabuhan',
       'biaya social', 'margin distributor', 'wh allowance bpdd', 'pph', 'ppn',
       'channel_trx', 'net margin ics', 'revenue', 'penj net', 'cont margin',
       'gross margin', 'net margin', 'net margin grp', 'profit', 'last_price',
       'harga jual sub dist', 'harga reguler', 'harga tebus incl tax',
       'harga tebus excl tax', 'opco md excl tax', 'opco md netto excl tax',
       'termofpayment', 'model',
       'simulation_status', 'flag_change', 'group_pelanggan', 'ship_to_name',
       'ship_to_code', 'material_type', 'volume_sig_prov',
       'last_sow_sig_prov', 'cbp_sig_prov', 'brand_nbc_prov', 'volume_nbc_prov', 'last_sow_nbc_prov', 'cbp_nbc_prov',
       'last_gap_volume_to_nbc_prov', 'last_gap_cbp_to_nbc_prov', 'last_gap_sow_to_nbc_prov',
       'avg_vol_6m', 'avg_vol_3m', 'last_sum_vol_2y',
       'last_sum_vol_1y', 'last_sum_vol_6m', 'last_freq_vol_2y',
       'last_freq_vol_1y', 'last_freq_vol_6m', 'plant_to_distance_sig',
       'plant_to_distance_sig_nbc',
       'province_kfold_target_enc',
       'material_type_kfold_target_enc', 'prediction_price',
       'prediction_volume','gpm','simulation_id']]

    return data_model


def calculate_gap_cbp(price,last_price):
    return price-last_price


def prep_vol_modelling_b2b(data_simulation, data_predict):
    data_model = get_flag_change(data_simulation,'Volume')
    data_predict = data_predict.groupby(['period','ship_to_name','ship_to_code','province','district','group_pelanggan','material_type']).first().reset_index()
    data_model = pd.merge(
        data_simulation,
        data_predict,
        right_on=['period', 'province', 'ship_to_code', 'material_type'],
        left_on=['period', 'province', 'ship to code', 'material type']
    )
    if len(data_model)>0:
    	material_type = data_model['material_type']
    	material_type = material_type.replace(' ','_')
    	material_type = material_type.lower()
    	data_model['gap_cbp_{0!s}'.format(material_type)] = list(map(lambda x,y:calculate_gap_cbp(x,y),data_model['prediction_price'],data_model['last_price']))
    	return data_model
    else:
    	return data_model


def prep_vol_modelling_retail(data_simulation, data_predict):
    data_simulation_retail = get_flag_change(data_simulation,'Volume')
    data_simulation_retail['brand_name'] = list(map(lambda x,y: grouping_entity(x,y),data_simulation_retail['entity'],data_simulation_retail['material type']))
    data_predict = data_predict.groupby(['period', 'district_name', 'province_name', 'district_ret','brand_name', 'kemasan']).first().reset_index()

    data_model = pd.merge(
        data_simulation_retail,
        data_predict,
        right_on=['period', 'district_name', 'kemasan', 'brand_name'],
        left_on=['period', 'district desc smi', 'packaging weight','brand_name']
    )
    if len(data_model)>0:
    	material_type = data_model['brand']
    	material_type = material_type.replace(' ','_')
    	material_type = material_type.lower()
    	data_model['gap_cbp_{0!s}'.format(material_type)] = list(map(lambda x,y:calculate_gap_cbp(x,y),data_model['prediction_price'],data_model['last_price']))
    	return data_model
    else:
    	return data_model


def loop_apply_model_retail(brand_name,data_model,model_elasticity,var_x):
	data_model_material = data_model[data_model['brand_name']==brand_name]
	algorithm = model_elasticity['material_type']
	file = open(F"./Modules/data/{algorithm}",'rb')
	volume_model = pickle.load(file)
	data_model_material['prediction_volume'] = volume_model.predict(data_model_material[var_x])
	return data_model_material


def apply_model_b2b(data_model, flag):
    if flag == 'Price':
        file = open("./Modules/data/rbp_model_b2b.pkl",'rb')
        price_model = pickle.load(file)
        file.close()
        var_x = [
        'plantdistancenbc','buying_vol_6mo','province_kfold_target_enc',
        'sow_sig','disparitas_volume','material_type_kfold_target_enc',
        'disparitas_sow','sow_nbc','buying_freq_6_mo',
        'disparitas_cbp','buying_vol_1yr','avg_buying_vol_3mo',
        'cbp_sig','avg_buying_vol_6mo','prediction_volume',
        'buying_freq_1yr','ship_to_name_kfold_target_enc','volume_sig',
        'plantdistancesig','cbp_nbc','volume_nbc',
        'buying_vol_2yr','buying_freq_2yr'
        ]
        data_model['prediction_price_simulation'] = price_model.predict(data_model[var_x])
        return data_model
    elif flag == 'Volume':
    	var_x = ['gap_cbp_pcc',
           'gap_cbp_opc_reguler', 'gap_cbp_opc_premium', 'gap_cbp_owc',
           'gap_cbp_putih', 'gap_cbp_pcc_premium', 'gap_cbp_type_v', 'gap_cbp_sbc',
           'gap_cbp_type_ii', 'gap_cbp_duramax', 'gap_cbp_maxstrength',
           'gap_cbp_ppc']
    	data_res = pd.DataFrame()
    	for material in data_model['material_type'].unique():
    		data_model_material = loop_apply_model_retail(material,data_model,model_elasticity,x)
    		data_res.append(data_model_material)
    	return data_res


def apply_model_retail(data_model, flag):
    if flag == 'Price':
        file = open("./Modules/data/model_prediction_retail_rbp.pkl",'rb')
        price_model = pickle.load(file)
        file.close()
        var_x = [
            'is_seasonality',
			'is_promo',
			'ms_brand',
			'volume_brand',
			'rbp_lm_nbc',
			'stok_level',
			'growth_rbp_3month',
			'growth_rbp_6month',
			'growth_volume_1month',
			'kemasan_kfold_target_enc',
			'brand_name_kfold_target_enc'
        ]
        data_model['prediction_price_simulation'] = price_model.predict(data_model[var_x])
        return data_model
    elif flag == 'Volume':
        var_x = ['gap_cbp_andalas', 'gap_cbp_dynamix', 'gap_cbp_masonry','gap_cbp_powermax', 
            'gap_cbp_sg', 'gap_cbp_sp', 'gap_cbp_st', 'disparitas_rbp_nbc', 'vol_min1',
            'district_name_kfold_target_enc']
        data_res = pd.DataFrame()
        for brand in data_model['brand_name'].unique():
        	data_model_material = loop_apply_model_b2b(brand,data_model,model_elasticity,x)
        	data_res.append(data_model_material)
        return data_res


def get_cost_data(tabel_cost):
    statement = """
        select * from {0!s}
    """.format(tabel_cost)
    tabel_cost = pd.read_sql_query(statement, engine)
    return tabel_cost


def HargaTebusExcTax(entity,channel_trx,harga_tebus_incl_tax):
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


def calculate_cost(data_simulation, data_cost):
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

    data_simulation_cost_b2b['oa'] = data_simulation_cost_b2b['oa_y']
    data_simulation_cost_b2b['var prod'] = data_simulation_cost_b2b['var prod_y']
    data_simulation_cost_b2b['trn'] = data_simulation_cost_b2b['trn_y']
    data_simulation_cost_b2b['kmsn'] = data_simulation_cost_b2b['kmsn_y']
    data_simulation_cost_b2b['var packer'] = data_simulation_cost_b2b['var packer_y']
    data_simulation_cost_b2b['fix packer'] = data_simulation_cost_b2b['fix packer_y']
    data_simulation_cost_b2b['fix prod'] = data_simulation_cost_b2b['fix prod_y']
    data_simulation_cost_b2b['adum'] = data_simulation_cost_b2b['adum_y']
    data_simulation_cost_b2b['sales'] = data_simulation_cost_b2b['sales_y']
    data_simulation_cost_b2b['company code/opco'] = data_simulation_cost_b2b['company code/opco_y']

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
    'ship to name_x','material type_x'
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

    data_simulation_cost_retail['oa'] = data_simulation_cost_retail['oa_y']
    data_simulation_cost_retail['var prod'] = data_simulation_cost_retail['var prod_y']
    data_simulation_cost_retail['trn'] = data_simulation_cost_retail['trn_y']
    data_simulation_cost_retail['kmsn'] = data_simulation_cost_retail['kmsn_y']
    data_simulation_cost_retail['var packer'] = data_simulation_cost_retail['var packer_y']
    data_simulation_cost_retail['fix packer'] = data_simulation_cost_retail['fix packer_y']
    data_simulation_cost_retail['fix prod'] = data_simulation_cost_retail['fix prod_y']
    data_simulation_cost_retail['adum'] = data_simulation_cost_retail['adum_y']
    data_simulation_cost_retail['sales'] = data_simulation_cost_retail['sales_y']
    data_simulation_cost_retail['company code/opco'] = data_simulation_cost_retail['company code/opco_y']
    data_simulation_cost_retail['ship to name'] = data_simulation_cost_retail['ship to name_y']
    data_simulation_cost_retail['ship to code'] = data_simulation_cost_retail['ship to code_y']
    data_simulation_cost_retail['material type'] = data_simulation_cost_retail['material type_y']

    data_simulation_cost_retail_column = set(data_simulation_cost_retail.columns)-set(drop_column)
    data_simulation_cost_retail = data_simulation_cost_retail[data_simulation_cost_retail_column]

    data_simulation_cost=data_simulation_cost_retail.append(data_simulation_cost_b2b,ignore_index=True)

    
    data_simulation_cost['revenue'] = (data_simulation_cost['prediction_price']*(data_simulation_cost['prediction_volume']/1000))*1000
    data_simulation_cost['penj net'] = data_simulation_cost['prediction_price']-data_simulation_cost['oa']
    data_simulation_cost['cont margin'] = data_simulation_cost['penj net']-(data_simulation_cost['var prod']+data_simulation_cost['trn']+data_simulation_cost['kmsn']+data_simulation_cost['var packer'])
    data_simulation_cost['gross margin'] = data_simulation_cost['cont margin']-(data_simulation_cost['fix packer']+data_simulation_cost['fix prod'])
    data_simulation_cost['net margin'] = data_simulation_cost['gross margin']-data_simulation_cost['adum']-data_simulation_cost['sales']
    data_simulation_cost['net margin grp'] = data_simulation_cost['net margin']+data_simulation_cost['net margin ics']
    data_simulation_cost['profit'] = data_simulation_cost['net margin grp']*data_simulation_cost['prediction_volume']
    data_simulation_cost['harga jual sub dist'] = data_simulation_cost['prediction_price']+data_simulation_cost['oa ke customer']+data_simulation_cost['opt']+data_simulation_cost['freight n container']+data_simulation_cost['freight']+data_simulation_cost['opp']+data_simulation_cost['oa to pelabuhan']+data_simulation_cost['biaya social']+data_simulation_cost['com']+data_simulation_cost['margin distributor']
    data_simulation_cost['harga reguler'] = data_simulation_cost['prediction_price'] + data_simulation_cost['wh allowance bpdd']
    data_simulation_cost['harga tebus incl tax'] = data_simulation_cost['prediction_price'] - data_simulation_cost['margin distributor']
    data_simulation_cost['harga tebus excl tax'] = list(map(lambda a,b,c: HargaTebusExcTax(a,b,c),data_simulation_cost['entity'],data_simulation_cost['channel_trx'],data_simulation_cost['harga tebus incl tax']))
    data_simulation_cost['pph'] = list(map(lambda a,b,c: pph(a,b,c),data_simulation_cost['entity'], data_simulation_cost['channel_trx'], data_simulation_cost['harga tebus excl tax']))                                                                                       
    data_simulation_cost['ppn']=data_simulation_cost['harga tebus excl tax']*0.1
    data_simulation_cost['opco md excl tax'] = list(map(lambda a,b,c,d,e,f,g,h,i,j,k:OpcoMDExTax(a,b,c,d,e,f,g,h,i,j,k),data_simulation_cost['entity'],data_simulation_cost['harga tebus excl tax'],data_simulation_cost['var prod'],data_simulation_cost['var packer'],data_simulation_cost['kmsn'],data_simulation_cost['fix prod'],data_simulation_cost['fix packer'],data_simulation_cost['trn'],data_simulation_cost['oa'],data_simulation_cost['material type'],data_simulation_cost['packaging mode']))
    data_simulation_cost['opco md netto excl tax'] = list(map(lambda a,b,c,d,e:OpcoMDNettoExTax(a,b,c,d,e),data_simulation_cost['entity'],data_simulation_cost['opco md excl tax'],data_simulation_cost['oa'],data_simulation_cost['com'],data_simulation_cost['biaya lain']))
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


