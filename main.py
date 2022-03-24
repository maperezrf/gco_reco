import pandas as pd
from datetime import datetime
import numpy as np
from tables import Column
import constants as const
from os import listdir
dt_string = datetime.now().strftime('%y%m%d')


class RECOBRO():

    planilla = None
    cf11 = None
    sap = None
    conciliaciones = None

    def __init__(self) -> None:
        pass


    def load_files(self):
        # TODO cargar todos los archivos
        pass

    def transform(self):
        # TODO transformar los archivos
        pass

    def f4_filters(self):
        #TODO llamar todos los filtros aquí 
        pass
    
    def get_transportadoras(self):
        pass


# General methods
# TODO completar los siguientes filtros
def fltr_dado_baja(df):
    pass

def fltr_reservado(df):
    pass

def fltr_fecha_desde(df):
    pass

def fltr_recobro(df):
    pass

def load_sap_files(df):
    pass

# ---- Hasta aquí PAP 

f4_name = input("Ingrese el nombre de la planilla F4: ")
carpeta_corte = input("Ingrese el nombre de la carpeta del corte: ")
carpeta_sap = input("Ingrese el nombre de la carpeta con los archvos SAP: ")

# Cargar datos
f4 = pd.read_csv(f'planillas/{f4_name}.csv', sep=';', dtype=str)
f4['total_precio_costo'] = pd.to_numeric(f4['total_precio_costo'], downcast= 'integer')

# Cambiar formatos
fechas = ['fecha_creacion', 'fecha_reserva']
f4.loc[:, fechas] = f4[fechas].apply(lambda x: x.replace(["ene", "abr", "ago", "dic"], ["jan", "apr", "aug", "dec"], regex=True))
for i in fechas: f4[i] = pd.to_datetime(f4[i])

   
f4 = f4.sort_values("fecha_reserva")
f4['mes'] = f4['fecha_reserva'].dt.strftime('%b')

f4.loc[f4.local.isin(const.tienda), 'local_agg'] = 'TIENDA'
f4.loc[f4.local.isin(const.cd), 'local_agg'] = 'CD'
f4.loc[f4.local == "3001", 'local_agg'] = 'DVD ADMINISTRATIVO'
f4.loc[f4.local == "99", 'local_agg'] = 'ADMINISTRATIVO'
f4.loc[f4.local == "11", 'local_agg'] = 'VENTA EMPRESA'
f4.loc[f4.local.isin(['3004', '3009']), "local_agg"] = 'EXPRESS'

f4_dado_baja = f4.loc[f4.tipo_redinv == "dado de baja"] 

f4_dado_baja_reservado = f4_dado_baja.loc[f4_dado_baja.estado =='reservado']
f4_2022 = f4_dado_baja_reservado.loc[(f4_dado_baja_reservado.fecha_reserva >='2022-01-01')].reset_index(drop=True)
f4_dado_baja_reservado = f4_dado_baja.loc[f4_dado_baja.estado =='reservado'] # TODO OJO: esto está duplicado 
f4_2022 = f4_dado_baja_reservado.loc[(f4_dado_baja_reservado.fecha_reserva >='2022-01-01')].reset_index(drop=True) # TODO OJO: esto está duplicado 

# TODO el archivo inicial debería ser el que ya esta clasificado, entonces las siguientes 4 lineas no sería necesar
f4_2022['Posible Causa'] = np.nan
print(f'Existen {f4_2022.shape[0]:,.0f} registros sin clasificar de 2021 por un costo de {f4_2022.loc[f4_2022["Posible Causa"].isna(), "total_precio_costo"].sum()/1e6} M')
f4_2022.loc[~f4_2022.destino.str.contains("sin",na=False) & (f4_2022.local_agg == "CD") & f4_2022.destino.str.contains(r'cobro\b.*\d{10}|\d{10}.*cobro\b|recupero|recobro\b.*\d{10}|\d{10}.*recobro\b|recobrado\b.*\d{10}|\d{10}.*recobrado\b'), 'Posible Causa'] = 'Recobro a transportadora'
print(f'Cantidad restante de registros sin clasificar posible causa: {f4_2022.loc[f4_2022["Posible Causa"].isna()].shape[0]}')

f4_reco_trans = f4_2022.loc[f4_2022['Posible Causa'] == "Recobro a transportadora"].reset_index(drop=True)

# TODO cambiar las primeras letras de los nombres de transportadoras a mayusculas
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('depr',na=False), "transportadora"] = "deprisa"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('servien',na=False), "transportadora"] = "servientrega"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains(r'rotterdan|roterdan',na=False), "transportadora"] = "rotterdan"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('linio',na=False), "transportadora"] = "linio"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('tcc',na=False), "transportadora"] = "tcc"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('aldia',na=False), "transportadora"] = "aldia"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('logysto',na=False), "transportadora"] = "logysto"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('empresari',na=False), "transportadora"] = "empresarial"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('teclogi',na=False), "transportadora"] = "teclogi"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('agil cargo',na=False), "transportadora"] = "agil cargo"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('mensajer',na=False), "transportadora"] = "mensajeros"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('vueltap',na=False), "transportadora"] = "vueltap"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('exxe',na=False), "transportadora"] = "exxe"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('integra',na=False), "transportadora"] = "integra"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('corona',na=False), "transportadora"] = "corona"
f4_reco_trans.loc[f4_reco_trans.destino.str.contains('suma',na=False), "transportadora"] = "suma"
# f4_reco_trans -> Df planilla f4 posible causa cobro a transportadora, y clasificado con transportadoras

print(f"Cantidad restante de registros sin clasificar transportadora: {f4_reco_trans.loc[f4_reco_trans['transportadora'].isna(),'nro_red_inventario'].count()}")

# TODO el siguiente método va en "def load_sap_files(df):"
archivos_sap = pd.DataFrame()
for file in listdir(f"{carpeta_corte}/{carpeta_sap}"):
    files = pd.read_excel(f'{carpeta_corte}/{carpeta_sap}/{file}',dtype=str)
    if files.columns[0] == 'Cliente':
        files.rename(columns={"Cliente":"Proveedor_cliente","Nombre de deudor":"Nombre de deudor_provedor"},inplace=True)
    else:
        files.rename(columns={"Proveedor":"Proveedor_cliente","Nombre del proveedor":"Nombre de deudor_provedor"},inplace=True)
        files = files.reindex(columns=['Proveedor_cliente', 'Status compens.', 'Fecha registr.diario',
       'Asiento contable', 'Tp.asiento contable', 'Importe (mon.soc.)',
       'Asiento compensación', 'AC creado por', 'Base de descuento',
       'Base reten.impuestos', 'Clave referen.3', 'Clave referencia 1',
       'Fecha compensación', 'Ind.impuestos', 'Ingresos facturados',
       'Nombre de deudor_provedor', 'Número de cuenta', 'Población', 'Referencia',
       'Referencia a factura', 'Referencia de pago', 'Texto partida'])
    archivos_sap = pd.concat([archivos_sap,files]) 
# archivos_sap -> unificacion archivos SAP

info_conciliaciones = pd.read_excel(f'{carpeta_corte}/220315_base_cd.xlsx', dtype = str, sheet_name='BASE')
info_conciliaciones = info_conciliaciones[['F4','DOCUMENTO CONTABLE', 'Nº  CONTABLE ']]
info_conciliaciones.drop_duplicates('F4', inplace=True)
f4_reco_trans_con_dc = f4_reco_trans.merge(info_conciliaciones, how='left', left_on='nro_red_inventario', right_on='F4')

archivos_sap = archivos_sap [["Importe (mon.soc.)","Referencia"]].reset_index()
archivos_sap.drop_duplicates(['Referencia'],inplace = True)
archivos_sap["Referencia"] = archivos_sap.Referencia.str.replace("-","")
archivos_sap.fillna(0, inplace=True)
recobro =f4_reco_trans_con_dc.merge(archivos_sap,how="left", left_on = 'Nº  CONTABLE ', right_on = "Referencia")
recobro.loc[recobro.Referencia.isna()]
recobro.loc[recobro.Referencia.notna(), "Referencia"] = "Encontrado en SAP"
recobro.loc[recobro.Referencia.isna(), "Referencia"] = "No encontrado en SAP"
recobro.rename(columns={"Referencia":"Indicador SAP"}, inplace=True)
recobro.to_excel(f"220317 corte/output/{dt_string}_resultado_recobro.xlsx",index =False) 

# TODO en este archivo no se está realizando la parte 2 de comparar contra los cierres de f11, tampoco se está comparando con las conciliaciones
# la idea es poder conocer y guardar que es las diferencias entre los archivos anterirores 