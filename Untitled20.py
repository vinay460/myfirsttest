#!/usr/bin/env python
# coding: utf-8

# In[130]:


import pandas as pd
import ast


# In[131]:


df=pd.read_csv('t.csv',sep=';')
df['filename']=df['dbname']+'.'+df['tablename']
filenames=df['filename'].unique().tolist()


# In[119]:


def fun(f):
    s=df[df['filename']==f].rawdata.values.tolist()
    s1=[eval(item) for item in s]
    df2=pd.DataFrame(s1)
    df2.to_csv(f,index=False)
    return f+' tables generated'


# In[ ]:


k=[ fun(f) for f in filenames]


# In[129]:


print(k)

