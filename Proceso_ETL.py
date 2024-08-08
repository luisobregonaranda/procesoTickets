import polars  as pl
from polars import Config
from pathlib import Path
from MyPackage.misFunciones import texto_a_decimal
from datetime import date

Config.set_fmt_str_lengths(100)

#LEctura de Tickets Historico
def importarTicketsHistorico(ruta: str,sep:str,columnas:list)-> pl.DataFrame:
    Historico=pl.read_csv(source=ruta,separator=sep,
                      has_header=True,
                      columns=columnas,
                      try_parse_dates=True ,  ##intenta parsear a fecha
                      ignore_errors=True     ##pone null a  los errores
                     )
    Historico=Historico.rename({'Numero Ticket': 'TicketID'})

    return Historico

print(importarTicketsHistorico(ruta='./Tickets/Tickets Historico.txt',sep=";",
                    columnas=['Numero Ticket','Ubicacion','Service Desk','Estado','Fecha Creacion','Fecha Termino','Fecha Cierre'],
                        ).head()
)
print('----------')
#Lectura de Tickets Actual

def importarTicketsActuales(ruta: str,sep:str,columnas:list)-> pl.DataFrame: 
     Actual=pl.read_csv(source=ruta,separator=sep,
                    columns=columnas
             
                   )
     Actual=Actual.select(
        pl.col('Numero Ticket').alias('TicketID'),
        pl.col(['Ubicacion','Service Desk','Estado']),
        pl.col('Fecha Creacion').str.to_date('%Y-%m-%d'),
        pl.col('Fecha Termino').str.to_date('%d/%m/%Y'),
        pl.col('Fecha Cierre').str.to_date('%d/%m/%Y'))
     return Actual





def transformacionTickets(hist:pl.DataFrame,act:pl.DataFrame)->pl.DataFrame:
     tickets=pl.concat([hist,act],how="vertical")

     ##filtrando data que solo sean WO

     tickets=tickets.filter(pl.col('TicketID').str.starts_with('WO') )##PARA NEGAR 126 ~
    ## filtrar por ticket y la fecha de creacion y quedarse con la ultima fecha la mas actual y mantener el orden
     tickets=tickets.sort(by=['TicketID','Fecha Creacion']).unique(
     subset='TicketID',
     keep="last",
     maintain_order=True)
     
    #Separando la columna Ubicacion
     tickets=tickets.with_columns(  pl.col('Ubicacion').str.split_exact(' - ',1).
     struct.rename_fields(['Agencia','AgenciaID'])
     ).unnest('Ubicacion').cast({"AgenciaID":pl.Int64})
     
     tickets=tickets.with_columns(pl.coalesce(['Fecha Termino','Fecha Cierre']).alias('Fecha Real Fin'))
    
    
     tickets=tickets.with_columns((pl.col('Fecha Real Fin')-pl.col('Fecha Creacion')).dt.total_days().alias("Dias Cierre"))
     ##crear columna grupo dias,
     tickets=tickets.with_columns(pl.when(pl.col("Dias Cierre").is_null()).then(None)
    .when(pl.col('Dias Cierre')<3).then(pl.lit('0 a 3 dias'))
    .when(pl.col('Dias Cierre')<7).then(pl.lit('3 a 7 dias'))
    .when(pl.col('Dias Cierre')<15).then(pl.lit('7 a 15 dias'))
    .otherwise(pl.lit('+ de 15 dias')).alias("Grupo Dias")
     )
     return tickets



def importarAtenciones(ruta:str) ->pl.DataFrame:
    rutaCarpeta=Path(ruta)
     #archivos de la carpeta
    archivosExcel=list(rutaCarpeta.glob('*.xlsx'))
    Atenciones=pl.DataFrame()
    for archivo in archivosExcel:
        file=archivo.name
        filepath=f'{ruta}/{file}'

        aux_df=pl.read_excel(
            source=filepath,
            engine="xlsx2csv",
            read_options={
            "columns":["Numero Ticket","Tipo de Ticket","Proveedor","Costo Atencion"],
            "dtypes": {"Costo Atencion":pl.Utf8}
            }
        )
        aux_df=aux_df.with_columns(pl.lit(file).alias('Nombre Archivo'))
        Atenciones=pl.concat([Atenciones,aux_df],how="vertical")

    return Atenciones
    
def transformacionAtenciones(data:pl.DataFrame)->pl.DataFrame:
    ##transformando data atenciones

    data=data.select(
        pl.col("Numero Ticket").alias("TicketID"), 
        'Tipo de Ticket','Proveedor',
        pl.col('Costo Atencion').str.replace(',','.').str.replace_many(['COSTO CERO','SIN COSTO'],'0')
        )
    #aplicando la function 
    data=data.with_columns(
        pl.col('Costo Atencion').map_elements(texto_a_decimal,return_dtype=float).alias('Costo Atencion') )
    
    return data

##no devuelve nada 
def ConsolidadoTicketsAtenciones(tic:pl.DataFrame,ate:pl.DataFrame,filename:str)->None:
        resultado=tic.join(
        ate,
        left_on='TicketID',
        right_on='TicketID',
        how='inner'
        ).select(
            'TicketID',
            'AgenciaID',
            'Agencia','Service Desk','Estado','Fecha Creacion','Fecha Real Fin','Grupo Dias',
            pl.col('Tipo de Ticket').alias('Tipo Ticket'),
            pl.col('Costo Atencion').alias('Costo')
        )

        try:
             fecha=date.today().strftime('%Y%m%d')
             nombre=f'{filename}_{fecha}.xlsx'
             resultado.write_excel(
                workbook=nombre,
                worksheet='Consolidado',
                autofit=True,
                table_style='Table Style Medium 4',
                dtype_formats={pl.Date:'dd/mm/yyyy'},   ##las columas tipos fechas entre otros
                float_precision=1,  ##un solo decimal
                column_totals={'Costo':'sum'})
             print('Se exporto el archivo correctamente.')
        except Exception as e:
             print(f'No se exporto el archivo: {e}')

if __name__== '__main__':
   rutaHistorico='./Tickets/Tickets Historico.txt'
   rutaActual='./Tickets/Tickets Actual.csv'
   rutaAteciones='./Atenciones'
   seph=";"
   columnas=['Numero Ticket','Ubicacion','Service Desk','Estado','Fecha Creacion','Fecha Termino','Fecha Cierre']

   hist=importarTicketsHistorico(ruta=rutaHistorico,sep=seph,columnas=columnas)
   act=importarTicketsActuales(ruta=rutaActual,sep='|',columnas=columnas)
   tickets=transformacionTickets(hist,act)
   atenciones=importarAtenciones(rutaAteciones)
   atenciones=transformacionAtenciones(atenciones)
   ConsolidadoTicketsAtenciones(tickets,atenciones,'sesion04')

   