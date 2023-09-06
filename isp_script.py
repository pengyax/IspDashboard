import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import calendar

def generate_lastday(ym):
    date = datetime.strptime(ym, '%Y-%m')
    days = calendar.monthrange(date.year, date.month)[1]
    date = date.replace(day=days)
    date = date.strftime('%Y-%m-%d')
    return date
    

def raw_process(df_jp,df_eu,df_anz,mapping_name,mapping_Exemption,std = '2022-01-01'):
    df_anz = (
    df_anz
    .query('`Date Complaint Received` >= @std')
    .assign(**{"Product Division" : lambda d : d['Product Division'].str.extract(r"(\d+)")[0].str[:2]})
    .loc[:,~df_anz.columns.str.contains('Unnamed')]
    .rename(columns=lambda x: x.replace('\n', ''))
    .assign(If_mfg_complaints = 'Y',
            VendorName = lambda d : d['Supplier'].map(mapping_name),
            Exemption = lambda d : d['Supplier'].map(mapping_Exemption))
    .query('~VendorName.isnull()')
    )

    df_eu = (
    df_eu
    .query('Date >= @std')
    # .assign(Division = lambda d : d['Prod-Line Code'].str.extract(r"(\d+)")[0].str[:2])
    .loc[:,eu_rename.keys()]
    .assign(If_mfg_complaints = 'Y',
            VendorName = lambda d : d['Vendor'].map(mapping_name),
            Exemption = lambda d : d['Vendor'].map(mapping_Exemption))
    )

    df_jp = (
    df_jp
    .query('`Receipt Date` >= @std')
    .assign(Division = lambda d : d['Division'].str.extract(r"(\d+)")[0].str[:2])
    .assign(If_mfg_complaints = lambda d : d.apply(lambda s : 'Y' if s["QA：責任"] in (['In-process','Component']) and s['Issued by'] == 'Customer' else 'N', axis = 1),
            VendorName = lambda d : d['Facility'].map(mapping_name),
            Exemption = lambda d : d['Facility'].map(mapping_Exemption))
    )
    print('JP', df_jp['Exemption'].isna().sum())
    print('ANZ', df_anz['Exemption'].isna().sum())
    print('EU', df_eu['Exemption'].isna().sum())
    
    with pd.ExcelWriter(f'./rawData.xlsx') as writer:
        df_jp.to_excel(writer, sheet_name="JAPAN", index=False)
        df_anz.to_excel(writer, sheet_name="ANZ", index=False)
        df_eu.to_excel(writer, sheet_name="EU", index=False)

    return df_jp,df_anz,df_eu

def cartesian (Market,Div,stdate,eddate):
    date_range = pd.date_range(start=stdate,end=eddate,freq='MS')
    index = pd.MultiIndex.from_product([Div,date_range,Market], names=['Division','Date', 'Market',])
    df_base = (pd.DataFrame(index=index)
               .reset_index()
               .assign(Year = lambda d : d['Date'].dt.year,
                       Month = lambda d : d['Date'].dt.month)
    )
    return df_base

if __name__ == "__main__":

    df_raw_jp = pd.read_excel('../09/PQR2023_Aug_Medline Vendor Asia.xlsx',sheet_name=2,usecols=list(range(0,18)),skiprows=2)
    df_raw_eu = pd.read_excel('../09/EU complaint details.xlsx')
    df_raw_anz = pd.read_excel('../09/ANZ Product Complaints Summary.xlsx')
    mapping_list = pd.read_excel('../VendorNameMapping.xlsx',sheet_name=0)
    mapping_name = dict(zip(mapping_list['Facility'],mapping_list['Vendor Name']))
    mapping_Exemption = dict(zip(mapping_list['Facility'],mapping_list['Exemption'])) 
     
    jp_rename = {
    'PQR No.':'Complaint No',
    'Issued by':'Issued by',
    'Receipt Date':'Receipt Date',
    'Division':'Division',
    'Item code':'Item Code',
    'Lot number':'Lot Number',
    'Manufacturing Year & Month':'Manufacturing Year & Month',
    'Defect Levels':'Defect Levels',
    'Defect Category':'Defect Category',
    '【レポート用】大分類':'Root Cause',
    'Description':'Complaint Description',
    'Priority Levels':'Priority Levels',
    'Date Sent':'Date Sent',
    'Due Date':'Due Date',
    'Date Reported':'Date Reported',
    'On-time response rate to SCAR':'On-time response rate to SCAR',
    'Facility':'Facility',
    'QA：責任':'QA：責任'}
    eu_rename = {
    'PQC':'Complaint No',
    'Date':'Receipt Date',
    'Incident Date':'Incident Date',
    'Site':'Site',
    'Domain':'Domain',
    'Entered By':'Entered By',
    'Sales Rep.':'Sales Rep.',
    'AssignedTo':'AssignedTo',
    'Customer Type Code':'Customer Type Code',
    'Customer Type Description':'Customer Type Description',
    'Customer':'Customer',
    'Name':'Customer Name',
    'City':'City',
    'Item':'Item Code',
    'Description':'Item Description',
    'Batch':'Lot Number',
    'Legal Manufacturer':'Legal Manufacturer',
    'Component':'Component',
    'Description.1':'Description.1',
    'Prod-Line Code':'Prod-Line Code',
    'Division':'Division',
    'Prod-Line Name':'Prod-Line Name',
    'Short Descr.':'Defect Category',
    'Descr. US':'Complaint Description',
    'Vendor':'Vendor',
    'Close-Date':'Due Date',
    'Root Cause':'Root Cause',
    'Preventive/Corrective Action':'Priority Levels'
    }
    anz_rename = {
    'Complaint Number':'Complaint No',
    'Date Complaint Received':'Receipt Date',
    'Product Division':'Division',
    'Product Code':'Item Code',
    'Product Description':'Item Description',
    'Product Batch(es)':'Lot Number',
    'TGA Category':'Defect Levels',
    'Component Code(packs only)':'Component',
    'Defect Type (*may vary from different sites)':'Defect Category',
    'Complaint Description':'Complaint Description',
    'Close out Comment / Root Cause':'Root Cause',
    'Complaint Closed Date':'Due Date',
    'Country':'Country',    
    'Component Description':'Component Description',
    'No. of Faulty Units':'No. of Faulty Units',
    'Sample/Photo Received Date at ANZ':'Sample/Photo Received Date at ANZ',
    'TGA reference number (if applicable)':'TGA reference number (if applicable)',
    'Tech File / Legal Manufacturer':'Tech File / Legal Manufacturer',
    'Supplier/ Component Item Number':'Supplier/ Component Item Number',
    'Supplier Lot Number':'Supplier Lot Number',
    'Supplier':'Supplier'
    }
    sort_list = ['Market',
    'Complaint No',
    'VendorName',
    'Year',
    'Month',
    'Receipt Date',
    'Division',
    'Item Code',
    'Item Description',
    'Lot Number',
    'Manufacturing Year & Month',
    'Defect Levels',
    'Component',
    'Defect Category',
    'Complaint Description',
    'Root Cause',
    'Priority Levels',
    'Due Date',
    'If_mfg_complaints',
    'Exemption']
    
    df_jp,df_anz,df_eu = raw_process(df_raw_jp,df_raw_eu,df_raw_anz,mapping_name,mapping_Exemption)
    
    df_jp = (
    df_jp
    .rename(columns=jp_rename)
    .assign(Market = 'JAPAN')
    )
    
    df_anz = (
    df_anz
    .rename(columns=anz_rename)
    .assign(Market = 'ANZ')
    )
    
    df_eu = (
    df_eu
    .rename(columns=eu_rename)
    .assign(Market = 'EU')
    )
    
    
    stdate = '2022-01'
    eddate = '2023-08'
    
    last_date = generate_lastday(eddate)
    
    (
    pd.concat([df_anz,df_jp,df_eu])
    .assign(Year = lambda d : d['Receipt Date'].dt.year,
            Month = lambda d : d['Receipt Date'].dt.month)
    .loc[:,sort_list]
    .assign(Division = lambda d : d['Division'].map(lambda x: 0 if pd.isna(x) else int(x)))
#     .astype({'Division':'Int64'})
    .query('If_mfg_complaints == "Y" and Exemption != "Y" and `Receipt Date` <= @last_date')
    .to_excel('ISPdatabase.xlsx',index=False)
    )
    
    Market = ['ANZ','JAPAN','EU']
    Div = [10,12,14,15,17,18,20,21,22,29,30,32,33,34,35,40,41,42,50,51,52,55,60,65,70,71,72,75,80,81,82,0]
    
    cartesian(Market,Div,stdate,eddate).to_excel('cartesian.xlsx',index=False)