#! /usr/bin/python3

import pandas as pd
import os
import numpy as np

#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/'
path='E:/angela/'
#path='/home/mayijun/TRAVELSHED/'

nyc=['36005','36047','36061','36081','36085']


# Summary
df=pd.DataFrame()
for i in os.listdir(path):
    tp=pd.read_csv(path+i,dtype=str)
    tp=tp.set_index('blockid')
    df=pd.concat([df,tp],axis=1)
df.to_csv(path+'bk.csv',index=True)

bk=pd.read_csv(path+'bk.csv',dtype=str)
bk=bk.set_index('blockid')
loclist=sorted(bk.columns)
for i in bk.columns:
    bk[i]=pd.to_numeric(bk[i])
bk=bk.replace(999,np.nan)
bk['tractid']=[str(x)[0:11] for x in bk.index]
bk=bk.groupby(['tractid'])[loclist].median(skipna=True)
bk.to_csv(path+'ct.csv',index=True,na_rep='999')


