from pickletools import read_uint1
import pandas as pd
from datetime import date, datetime
from os import listdir
dt_string = datetime.now().strftime('%y%m%d')
from dics import f4_var

class RECOBRO():

    planilla = None
    cf11 = None
    sap = pd.DataFrame()
    conciliaciones = None

    def __init__(self) -> None: #[x]
        self.f4_name = '220718_f4' #input('Ingrese el nombre de la planilla F4: ')
        self.carpeta_corte = '220718' #input('Ingrese el nombre de la carpeta del corte: ')
        self.carpeta_sap = 'partidas_sap' #input('Ingrese el nombre de la carpeta con los archvos SAP: ')
        self.db_conciliaciones = '220630_recobro rev Kelly'
        self.db_cf11 = '220601-1358-cf11_cd_21-all'

        print('Cargando archivos ...')
        self.load_files()
        print('Transformando datos ...')
        self.transform()
        print('Filtrando datos ...')
        self.f4_reco = self.f4_filters()
        print('Obteniendo transportadoras ...')
        self.f4_reco = get_transportadoras(self.f4_reco)
        print('Eliminando columnas ...')
        self.f4_reco.drop([f4_var['destino']],axis=1, inplace=True)

        print('Ejecutando análisis ...')
        self.numeros_f4_reco = self.f4_reco[f4_var['nfolio']].unique()
        self.numeros_f4_conciliacion = self.conciliaciones[f4_var['nfolio']].unique()
        self.numeros_f4_cf11 = self.cf11['f4'].unique()

        self.init_plus()
        print('El restulado se guardó con éxito!')

    def init_plus(self): #[x]
        self.compare_cf11_f4()
        self.cruce_sap_conc()

    def load_files(self): #[x]
        self.planilla = pd.read_csv(f'input/{self.carpeta_corte}/{self.f4_name}.csv', dtype = 'object')
        self.conciliaciones = pd.read_excel(f'input/{self.carpeta_corte}/{self.db_conciliaciones}.xlsx', dtype = str, sheet_name='DB')
        self.cf11 = pd.read_excel(f'input/{self.carpeta_corte}/{self.db_cf11}.xlsx', dtype = str, sheet_name='DB')
        path = f'input/{self.carpeta_corte}/{self.carpeta_sap}'
        self.sap = load_sap_files(path)
    
    def get_recobro(self):
        return self.f4_reco
    
    def get_sap(self):
        return self.sap

    def get_cd(self):
        return self.conciliaciones

    def transform(self): # [x]
        self.planilla.loc[:,f4_var['destino']] = self.planilla.loc[:,f4_var['destino']].str.lower()
        self.planilla[f4_var['tc']] = pd.to_numeric(self.planilla[f4_var['tc']]) #, downcast= 'integer')
        self.planilla['LOCAL'] = self.planilla[f4_var['loc_id']] + ' - ' +self.planilla[f4_var['loc_name']]
        fechas = [f4_var['fr'], f4_var['fs']]
        for i in fechas:  self.planilla[i] = pd.to_datetime(self.planilla[i], format='%Y-%m-%d')

        self.planilla.dropna(axis=1, how='all', inplace=True)
        self.planilla.drop([ 'TNOTAS','COPERARIO', 'NFORMULARIO', 'BMARGEN', 'DESCESTADO', 'USR_ENVIO','FECHA_ENVIO', 'USR_ANULADO', 
        'FECHA_ANULADO', 'DESCTIPO', 'CCENT_COSTO_F', 'PRD_LVL_CHILD', 'LOC_ID', 'LOC_NAME','PROD_CAT_ID','PROD_CAT_DESC', 'LOCAL_AGG',
        'MF04_UNIT_CST', 'PROD_BRAND_ID', 'USR_REGISTRO', 'USR_RESERVA'],axis=1, inplace=True)

        #cols = ['CTECH_KEY',  'PRD_UPC', 'QF04_SHIP',  'TOTAL_COSTO', 'CESTADO',  'FECHA_REGISTRO',  'FECHA_RESERVA', 'PROD_NAME',  'CTIPO', 'XDESTINO','POSIBLE_CAUSA']

        # CF11
        self.cf11 = fltr_cf11_f4_recobro(self.cf11)

        #CD
        #self.conciliaciones.rename(columns={'TRANSPORTADORA ':'TRANSPORTADORA_CD', 'F4':f4_var['nfolio'], 'REFERENCIA':'sku_producto',
        #                'Nº  CONTABLE ':'ref_sap', 'COSTO_TOTAL':'costo_total_f11_cd'}, inplace=True)
        #self.conciliaciones['costo_total_f11_cd'] = pd.to_numeric(self.conciliaciones['costo_total_f11_cd'])
        self.conciliaciones.columns = [col.lower() for col in self.conciliaciones.columns]
        self.conciliaciones['ref_sap']= self.conciliaciones['ref_sap'].str.replace('-', '') 
        self.conciliaciones.rename(columns={'nro_red_inventario':f4_var['nfolio']}, inplace=True)

        #SAP
        self.sap.rename(columns={'Clave referen.3':'transportadora_sap', 'Referencia':'ref_sap'}, inplace=True)
        self.sap.columns = [col.lower() for col in self.sap.columns]
        self.sap.to_excel('sap.xlsx')
        self.sap['fecha registr.diario'] = pd.to_datetime(self.sap['fecha registr.diario'])
        self.sap['ref_sap'] = self.sap['ref_sap'].fillna('nan').str.replace('-','')
        self.sap.drop_duplicates('ref_sap',inplace = True) # TODO revisar si se requiere
        self.sap = self.sap.loc[self.sap['ref_sap'].notna()].reset_index(drop=True)
        self.sap['importe (mon.soc.)'] = pd.to_numeric(self.sap['importe (mon.soc.)'])

    def f4_filters(self): #[x]
        dado_de_baja = fltr_dado_baja(self.planilla)
        reservado = fltr_reservado(dado_de_baja)
        planilla_2022 = fltr_fecha_desde(reservado)
        f4_reco = fltr_recobro(planilla_2022)
        return f4_reco

    def compare_cf11_f4(self): #[x]
        self.f4_reco.loc[self.f4_reco[f4_var['nfolio']].isin(self.numeros_f4_cf11), 'ind_cf11'] = 'P-CF11'
        self.f4_reco.loc[~self.f4_reco[f4_var['nfolio']].isin(self.numeros_f4_cf11), 'ind_cf11'] = 'N-CF11' 

        f4_faltante_en_cf11 = self.f4_reco.loc[~self.f4_reco[f4_var['nfolio']].isin(self.numeros_f4_cf11)].reset_index(drop=True) # REPETIDO
        f4_faltante_en_f4 = self.cf11.loc[~self.cf11.f4.isin(self.numeros_f4_reco)].reset_index(drop=True)
        self.save_missing_values(f4_faltante_en_cf11, f4_faltante_en_f4, 'cf11')

    def save_missing_values(self, yf4_ndf, nf4_ydf, label): #[x]
        if yf4_ndf.shape[0] > 0: yf4_ndf.to_excel(f'output/{self.carpeta_corte}/{dt_string}_yf4_n{label}.xlsx', index=False)
        if nf4_ydf.shape[0] > 0: nf4_ydf.to_excel(f'output/{self.carpeta_corte}/{dt_string}_nf4_y{label}.xlsx',index=False)

    def get_dif_f4_cd(self): #[x]
        info_conciliaciones = self.conciliaciones[[f4_var['nfolio'], 'nfolio', 'doc_contable', 'ref_sap', 'enviado_contabilidad']].reset_index(drop=True)
        f4mcd = self.f4_reco.merge(info_conciliaciones, how='left', on=[f4_var['nfolio']]) #, 'sku_producto'])
        cdmf4 = self.f4_reco.merge(info_conciliaciones, how='right', on=[f4_var['nfolio']]) #, 'sku_producto'])
    
        f4mcd.loc[f4mcd['nfolio'].notna(), 'ind_mf4' ] = 'yf4-ycd'
        f4mcd.loc[f4mcd['nfolio'].isna(), 'ind_mf4' ] = 'yf4-ncd'
        cdmf4.loc[cdmf4[f4_var['estado']].isna(), 'ind_mf4' ] = 'nf4-ycd'

        yf4_ncd = f4mcd.loc[f4mcd['ind_mf4']=='yf4-ncd'].reset_index(drop=True)
        nf4_ycd = cdmf4.loc[cdmf4['ind_mf4']=='nf4-ycd'].reset_index(drop=True)
        writer = pd.ExcelWriter(f'output/{self.carpeta_corte}/{dt_string}_comparacion.xlsx', engine='xlsxwriter')
        if yf4_ncd.shape[0] > 0: yf4_ncd.to_excel(writer, sheet_name='yf4_ncd' , index =False) 
        if nf4_ycd.shape[0] > 0: nf4_ycd.to_excel(writer, sheet_name='nf4_ycd', index =False) 
        writer.save()

        return pd.concat([f4mcd, nf4_ycd]).reset_index(drop=True) 
        
    def cruce_sap_conc(self):
        archivos_sap = self.sap [['fecha registr.diario', 'asiento compensación', 'ac creado por','transportadora_sap', 
                    'tp.asiento contable', 'importe (mon.soc.)','ref_sap']].reset_index(drop=True)

        f4mcd = self.get_dif_f4_cd()
        recobro = f4mcd.merge(archivos_sap,how='left', on = 'ref_sap')
        recobro.loc[recobro['importe (mon.soc.)'].notna(), 'Indicador SAP'] = 'Encontrado en SAP'
        recobro.loc[recobro['importe (mon.soc.)'].isna(), 'Indicador SAP'] = 'No encontrado en SAP'

        text_cols = recobro.select_dtypes('object')
        recobro[text_cols.columns] = text_cols.fillna('nan')
        fecha_nan = pd.to_datetime('01-01-2000')
        date_cols = recobro.select_dtypes('datetime')
        recobro[date_cols.columns] = date_cols.fillna(fecha_nan)
        num_cols = recobro.select_dtypes('number')
        recobro[num_cols.columns] = num_cols.fillna(0)

        gb_recobro = recobro.groupby(['LOCAL', f4_var['fs'], f4_var['nfolio'], 'nfolio', 
                        'transportadora_f4', 'doc_contable', 'ref_sap', 'enviado_contabilidad', 
                        'fecha registr.diario', 'asiento compensación','ac creado por', 'transportadora_sap', 'tp.asiento contable',
                         'Indicador SAP', 'importe (mon.soc.)']).agg({f4_var['tc']:'sum'}).reset_index()
        gb_date_cols = gb_recobro.select_dtypes('datetime')
        gb_recobro[gb_date_cols.columns] = gb_date_cols.applymap(lambda x: pd.NaT if x == pd.to_datetime('01-01-2000', format='%d-%m-%Y') else x)

        gb_documentos = recobro.groupby(['doc_contable', 'ref_sap', 'enviado_contabilidad', 
                        'fecha registr.diario', 'asiento compensación','ac creado por', 'transportadora_sap', 'tp.asiento contable',
                         'Indicador SAP', 'importe (mon.soc.)']).agg({f4_var['tc']:'sum'}).reset_index()
        gb_documentos_date_cols = gb_documentos.select_dtypes('datetime')
        gb_documentos[gb_documentos_date_cols.columns] = gb_documentos_date_cols.applymap(lambda x: pd.NaT if x == pd.to_datetime('01-01-2000', format='%d-%m-%Y') else x)

        writer = pd.ExcelWriter(f'output/{self.carpeta_corte}/{dt_string}_resultado_recobro.xlsx', engine='xlsxwriter')
        
        gb_documentos.to_excel(writer, sheet_name='DC', index =False)
        gb_recobro.to_excel(writer, sheet_name='F4', index =False) 
        recobro.to_excel(writer, sheet_name='F4 Detalle' , index =False)  
        writer.save()

# General methods

def fltr_cf11_f4_recobro(df): #[x]
    return df.loc[df['status_final']=='Cierre x F4 cobrado a terceros'].reset_index(drop=True)

def fltr_dado_baja(df): #[x]
    return df.loc[(df[f4_var['tipo']] == '4') ].reset_index(drop=True) 

def fltr_reservado(df): #[x]
    return df.loc[(df[f4_var['estado']] =='2')].reset_index(drop=True) 

def fltr_fecha_desde(df): #[x]
    return df.loc[df[f4_var['fs']] >= '01-01-2022'].reset_index(drop=True)

def fltr_recobro(df): #[x]
    return df.loc[df[f4_var['ps']] == 'Recobro a transportadora'].reset_index(drop=True) 

def get_transportadoras(df): #[x]
    df.loc[df[f4_var['destino']].str.contains('depr'), 'transportadora_f4'] = 'Deprisa'
    df.loc[df[f4_var['destino']].str.contains('servien',na=False), 'transportadora_f4'] = 'Servientrega'
    df.loc[df[f4_var['destino']].str.contains(r'rotterdan|roterdan',na=False), 'transportadora_f4'] = 'Rotterdan'
    df.loc[df[f4_var['destino']].str.contains('linio',na=False), 'transportadora_f4'] = 'Linio'
    df.loc[df[f4_var['destino']].str.contains('tcc',na=False), 'transportadora_f4'] = 'Tcc'
    df.loc[df[f4_var['destino']].str.contains('aldia',na=False), 'transportadora_f4'] = 'Aldia'
    df.loc[df[f4_var['destino']].str.contains(r'logysto|vueltap',na=False), 'transportadora_f4'] = 'Logysto'
    df.loc[df[f4_var['destino']].str.contains('empresari',na=False), 'transportadora_f4'] = 'Empresariales'
    df.loc[df[f4_var['destino']].str.contains('teclogi',na=False), 'transportadora_f4'] = 'Teclogi'
    df.loc[df[f4_var['destino']].str.contains(r'agil cargo|a.cargo',na=False), 'transportadora_f4'] = 'Agil cargo'
    df.loc[df[f4_var['destino']].str.contains('mensajer',na=False), 'transportadora_f4'] = 'Mensajeros'
    df.loc[df[f4_var['destino']].str.contains('exxe',na=False), 'transportadora_f4'] = 'Exxe'
    df.loc[df[f4_var['destino']].str.contains('integra',na=False), 'transportadora_f4'] = 'Integra'
    df.loc[df[f4_var['destino']].str.contains('corona',na=False), 'transportadora_f4'] = 'Corona'
    return df 

def load_sap_files(path): #[x]
    lista_sap = []
    for file in listdir(path):
        sap_file = pd.read_excel(f'{path}/{file}',dtype=str)
        if sap_file.columns[0] == 'Cliente':
            sap_file.rename(columns={'Cliente':'Proveedor_cliente','Nombre de deudor':'Nombre de deudor_provedor'},inplace=True)
        else:
            sap_file.rename(columns={'Proveedor':'Proveedor_cliente','Nombre del proveedor':'Nombre de deudor_provedor'},inplace=True)
        
        sap_file = sap_file.reindex(columns=['Proveedor_cliente', 'Status compens.', 'Fecha registr.diario',
        'Asiento contable', 'Tp.asiento contable', 'Importe (mon.soc.)',
        'Asiento compensación', 'AC creado por', 'Base de descuento',
        'Base reten.impuestos', 'Clave referen.3', 'Clave referencia 1',
        'Fecha compensación', 'Ind.impuestos', 'Ingresos facturados',
        'Nombre de deudor_provedor', 'Número de cuenta', 'Población', 'Referencia',
        'Referencia a factura', 'Referencia de pago', 'Texto partida'])
        lista_sap.append(sap_file)
    return pd.concat(lista_sap)

recobro =  RECOBRO()