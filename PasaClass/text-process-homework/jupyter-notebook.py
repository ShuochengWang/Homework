
# coding: utf-8

# In[1]:


import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules


# In[2]:


fin = open("T10I4D100K.txt", "r")
dataset = [[int(n) for n in line.split()] for line in fin]


# In[3]:


te = TransactionEncoder()
te_ary = te.fit(dataset).transform(dataset, sparse=True)
sparse_df = pd.SparseDataFrame(te_ary, columns=te.columns_, default_fill_value=False)
sparse_df


# In[4]:


frequent_itemsets5 = apriori(sparse_df, min_support=0.5, use_colnames=True)
frequent_itemsets5


# In[5]:


frequent_itemsets1 = apriori(sparse_df, min_support=0.1, use_colnames=True)
frequent_itemsets1


# In[6]:


frequent_itemsets05 = apriori(sparse_df, min_support=0.05, use_colnames=True)
frequent_itemsets05


# In[8]:


rules05 = association_rules(frequent_itemsets05, metric="confidence", min_threshold=0.5)
rules05


# In[9]:


frequent_itemsets01 = apriori(sparse_df, min_support=0.01, use_colnames=True)
frequent_itemsets01


# In[10]:


rules01 = association_rules(frequent_itemsets01, metric="confidence", min_threshold=0.7)
rules01


# In[11]:


rules01 = association_rules(frequent_itemsets01, metric="confidence", min_threshold=0.5)
rules01


# In[12]:


rules01 = association_rules(frequent_itemsets01, metric="confidence", min_threshold=0.3)
rules01


# In[13]:


rules01 = association_rules(frequent_itemsets01, metric="confidence", min_threshold=0.1)
rules01


# In[14]:


import pyfpgrowth


# In[15]:


patterns = pyfpgrowth.find_frequent_patterns(dataset, 1000)
patterns


# In[18]:


rules5 = pyfpgrowth.generate_association_rules(patterns, 0.5)
rules5


# In[19]:


patterns500 = pyfpgrowth.find_frequent_patterns(dataset, 500)
patterns500


# In[20]:


rules500_5 = pyfpgrowth.generate_association_rules(patterns500, 0.5)
rules500_5

