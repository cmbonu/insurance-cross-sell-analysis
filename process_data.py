# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
import pandas as pd

tdata = pd.read_csv('train.csv')
tdata['Response'] = tdata['Response'].apply(lambda x : 'Y' if x==1 else 'N')

age_breaks = [20.0, 32.0, 46.0, 60.0, 85.0]
premium_breaks = [2630.0, 15955.0, 36292.0, 57361.0, 540165.0]
tdata['age_group'] = pd.cut(tdata['Age'],bins=age_breaks,labels=['age_1', 'age_2','age_3','age_4'],include_lowest=True)
tdata['premium_group'] = pd.cut(tdata['Annual_Premium'],bins=premium_breaks,labels=['prem_1', 'prem_2','prem_3','prem_4'],include_lowest=True)

response_value = tdata['Response'].unique()

def build_group_summary(source_df, summary_cols, identity_col,target_col,target_values,fnum):
    aggregate_columns = [identity_col]
    aggregate_columns.extend(summary_cols)
    aggregate_columns.append(target_col)
    
    group_cols = summary_cols.copy()
    group_cols.append(target_col)
    
    group_summary = source_df[aggregate_columns].groupby(by=group_cols).count().reset_index().dropna()
    #target_values = group_summary[target_col].unique()
    group_summary = pd.pivot_table(group_summary,index=summary_cols, columns=target_col, values=identity_col, aggfunc= 'sum').reset_index()
    group_summary[target_values] = group_summary[target_values].fillna(0)
    group_summary['total'] = group_summary.apply(lambda x : x[target_values].sum(),axis=1)

    group_summary_extd = group_summary.copy(deep=True)
    group_summary_extd['merged_columns_values'] = group_summary_extd.apply(lambda x : ' >>> '.join([str(y) for y in list(x[summary_cols])]),axis=1)
    group_summary_extd['merged_columns_names'] = ' >>> '.join(summary_cols)
    group_summary_extd['col_count'] = len(summary_cols)

    #custom impl
    group_summary_extd['pct'] = group_summary_extd.apply(lambda x : (x['Y']+1)/(x['total']+2),axis=1)
    
    return_columns = ['merged_columns_names','merged_columns_values']
    return_columns.extend(target_values)
    return_columns.append('total')
    return_columns.append('pct')
    return_columns.append('col_count')

    group_summary_extd[return_columns].to_csv(f'results/summary_{fnum}.csv',index=False)
    print(fnum)
    
    #return group_summary, group_summary_extd[return_columns]
    #return group_summary.pivot(index=summary_cols,values=identity_col,columns=target_col)

rdcols = ['Gender','Driving_License', 'Region_Code','Previously_Insured', 'Vehicle_Age', 'Vehicle_Damage','Policy_Sales_Channel', 'Vintage','premium_group','age_group']

from itertools import combinations

col_list = []
for i in range(1,5):
    for cols in combinations(rdcols,i):
        rcols = list(cols)
        col_list.append(rcols)
print(len(col_list))
import multiprocessing
if __name__ == '__main__':
    processes = []
    for i,col_grp in enumerate(col_list):
        p = multiprocessing.Process(target=build_group_summary, args=(tdata,col_grp,'id','Response',response_value,i,))
        processes.append(p)
        p.start()
        
    for process in processes:
        process.join()


