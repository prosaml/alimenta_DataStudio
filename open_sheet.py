#Funcion carga de hoja
def open_sheet(sheet,gdoc,gc):
    
    import pandas as pd
    book = gc.open_by_key(gdoc)
    worksheet = book.worksheet(sheet)
    a= worksheet.get_all_values()
    a=pd.DataFrame(a)
    a.columns = a.iloc[0]
    a=a.reindex(a.index.drop(0))
    return a