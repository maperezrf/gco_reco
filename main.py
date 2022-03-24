import pandas as pd
from datetime import datetime
import numpy as np
from tables import Column
from os import listdir
dt_string = datetime.now().strftime('%y%m%d')

class RECOBRO():

    planilla = None
    cf11 = None
    sap = pd.DataFrame()
    conciliaciones = None

    def __init__(self) -> None:
        self.f4_name = "220323_f4_clasificado" #input("Ingrese el nombre de la planilla F4: ")
        self.carpeta_corte = "220317 corte" #input("Ingrese el nombre de la carpeta del corte: ")
        self.carpeta_sap = "archivos sap" #input("Ingrese el nombre de la carpeta con los archvos SAP: ")
        self.load_files()
        self.transform()
        self.f4_reco = self.f4_filters()
        self.get_transportadoras()
        self.compare_cf11_f4()
        self.compare_base_cd_f4()
        self.load_sap_files()
        self.cruce_sap_conc()

    def load_files(self):
        self.planilla = pd.read_excel(f'planillas/{self.f4_name}.xlsx', dtype = str)
        self.conciliaciones = pd.read_excel(f'{self.carpeta_corte}/220315_base_cd.xlsx', dtype = str, sheet_name='BASE')
        self.cf11 = pd.read_excel(f'{self.carpeta_corte}/220307_cf11_cd.xlsx', dtype = str, sheet_name='DB')

    def transform(self):
        self.planilla['total_precio_costo'] = pd.to_numeric(self.planilla['total_precio_costo'], downcast= 'integer')
        fechas = ['fecha_creacion', 'fecha_reserva']
        for i in fechas:  self.planilla[i] = pd.to_datetime(self.planilla[i])

    def f4_filters(self):
        dado_de_baja = fltr_dado_baja(self.planilla)
        reservado = fltr_reservado(dado_de_baja)
        planilla_2022 = fltr_fecha_desde(reservado)
        f4_reco = fltr_recobro(planilla_2022)
        return f4_reco

    def get_transportadoras(self):
        self.f4_reco.loc[self.f4_reco.destino.str.contains('depr'), "transportadora"] = "Deprisa"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('servien',na=False), "transportadora"] = "Servientrega"
        self.f4_reco.loc[self.f4_reco.destino.str.contains(r'rotterdan|roterdan',na=False), "transportadora"] = "Rotterdan"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('linio',na=False), "transportadora"] = "Linio"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('tcc',na=False), "transportadora"] = "Tcc"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('aldia',na=False), "transportadora"] = "Aldia"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('logysto',na=False), "transportadora"] = "Logysto"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('empresari',na=False), "transportadora"] = "Empresarial"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('teclogi',na=False), "transportadora"] = "Teclogi"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('agil cargo',na=False), "transportadora"] = "Agil cargo"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('mensajer',na=False), "transportadora"] = "Mensajeros"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('vueltap',na=False), "transportadora"] = "Vueltap"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('exxe',na=False), "transportadora"] = "Exxe"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('integra',na=False), "transportadora"] = "Integra"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('corona',na=False), "transportadora"] = "Corona"
        self.f4_reco.loc[self.f4_reco.destino.str.contains('suma',na=False), "transportadora"] = "Suma"

    def compare_cf11_f4(self):
        self.cf11_recobro = self.cf11.loc[self.cf11['status_final']=='Cierre x F4 cobrado a terceros'].reset_index(drop=True)
        numeros_f4_reco = self.f4_reco.nro_red_inventario.unique()
        numeros_f4_cf11 = self.cf11_recobro.f4.unique()
        self.f4_reco.loc[self.f4_reco.nro_red_inventario.isin(numeros_f4_cf11), 'indicador_f4'] = 'P-CF11'
        self.f4_reco.loc[~self.f4_reco.nro_red_inventario.isin(numeros_f4_cf11), 'indicador_f4'] = 'N-CF11' 
        self.f4_reco.to_excel(f'{self.carpeta_corte}/output/{dt_string}f4_clasificado_vs_cf11.xlsx', index=False)
        f4_faltante_en_cf11 = self.f4_reco.loc[~self.f4_reco.nro_red_inventario.isin(numeros_f4_cf11)].reset_index()
        f4_faltante_en_cf11.to_excel(f'{self.carpeta_corte}/output/{dt_string}_f4_faltante_en_f11.xlsx', index=False)
        f4_faltante_en_f4 = self.cf11_recobro.loc[~self.cf11_recobro.f4.isin(numeros_f4_reco)].reset_index()
        if f4_faltante_en_f4.shape[0] > 0 :
            f4_faltante_en_f4.to_excel(f'{self.carpeta_corte}/output/{dt_string}_f4_faltante_en_plnilla_f4.xlsx', index=False)

    def compare_base_cd_f4(self):
        numeros_f4_reco = self.f4_reco.nro_red_inventario.unique()
        numeros_f4_conciliacion = self.conciliaciones.F4.unique()
        self.f4_reco.loc[self.f4_reco.nro_red_inventario.isin(numeros_f4_conciliacion), 'indicador_base_cd'] = 'P-base_cd'
        self.f4_reco.loc[~self.f4_reco.nro_red_inventario.isin(numeros_f4_conciliacion), 'indicador_base_cd'] = 'N-base_cd'
        self.f4_reco.to_excel(f"{self.carpeta_corte}/output/{dt_string}_f4_clasificado_vs_base_cd.xlsx", index=False)
        f4_faltante_en_base_cd = self.conciliaciones.loc[~self.conciliaciones.F4.isin(numeros_f4_reco)].reset_index()
        if f4_faltante_en_base_cd.shape[0] > 0:
            f4_faltante_en_base_cd.to_excel(f"{self.carpeta_corte}/output/{dt_string}_f4_clasificado_vs_base_cd.xlsx",index=False)
        
    def load_sap_files(self):
        for file in listdir(f"{self.carpeta_corte}/{self.carpeta_sap}"):
            files = pd.read_excel(f'{self.carpeta_corte}/{self.carpeta_sap}/{file}',dtype=str)
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
            self.sap = pd.concat([self.sap,files])

    def cruce_sap_conc(self):
        info_conciliaciones = self.conciliaciones[['F4','DOCUMENTO CONTABLE', 'Nº  CONTABLE ']].reset_index()
        info_conciliaciones.drop_duplicates('F4', inplace=True)
        self.f4_reco_con_dc = self.f4_reco.merge(info_conciliaciones, how='left', left_on='nro_red_inventario', right_on='F4')
        archivos_sap = self.sap [["Importe (mon.soc.)","Referencia"]].reset_index()
        archivos_sap.drop_duplicates('Referencia',inplace = True)
        archivos_sap["Referencia"] = archivos_sap.Referencia.str.replace("-","")
        archivos_sap.fillna(0, inplace=True)
        recobro = self.f4_reco_con_dc.merge(archivos_sap,how="left", left_on = 'Nº  CONTABLE ', right_on = "Referencia")
        recobro.loc[recobro.Referencia.notna(), "Referencia"] = "Encontrado en SAP"
        recobro.loc[recobro.Referencia.isna(), "Referencia"] = "No encontrado en SAP"
        recobro.rename(columns={"Referencia":"Indicador SAP"}, inplace=True)
        recobro.to_excel(f"220317 corte/output/{dt_string}_resultado_recobro.xlsx",index =False) 

# General methods
def fltr_dado_baja(df):
    return df.loc[(df.tipo_redinv == "dado de baja") ].reset_index(drop=True) 

def fltr_reservado(df):
    return df.loc[(df.estado =='reservado')].reset_index(drop=True) 

def fltr_fecha_desde(df):
    return df.loc[(df.estado =='reservado')].reset_index(drop=True) 

def fltr_recobro(df):
    return df.loc[df['Posible Causa'] == 'Recobro a transportadora'].reset_index(drop=True) 

# ---- Hasta aquí PAP 

recobro =  RECOBRO()