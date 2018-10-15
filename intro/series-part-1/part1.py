import dask.dataframe as dd
from datetime import datetime
import numpy as np
from dask.diagnostics import ProgressBar

df = dd.read_csv('./dc-pass-data.csv')
print(df.describe)

# Produces an error if schema is not definite due to lazy evaluation
# print(df.head())

dtypes = {
    'CONTACTID': np.str,
    'TITLE': np.str,
    'TOTALOPTIONYEARS': np.int8,
    'CITYWIDE': np.str,
    'AGENCY': np.str,
    'AGENCYNAME': np.str,
    'PROCUREMENTMETHOD': np.str,
    'CONTRACTTYPE': np.str,
    'MARKETTYPE': np.str,
    'AMOUNT': np.float64,
    'LASTMODIFIED': np.str,
    'CONTRACTSTATUS': np.str,
    'AWARDDATE': np.str,
    'EFFECTIVEDATE': np.str,
    'EXPIRATIONDATE': np.str,
    'OWNER': np.str,
    'CONTRACTINGOFFICER': np.str,
    'SUPPLIER': np.str,
    'AWARDDOC': np.str,
    'COMMODITY': np.str,
    'SOLICITATIONID': np.str,
    'DSC_LAST_MOD_DTTM': np.str,
    'OBJECTID': np.int64
}

df = dd.read_csv('./dc-pass-data.csv', dtype=dtypes)

with ProgressBar():
    print(df.head())

missing_values = df.isnull().sum()
missing_count = ((missing_values / df.index.size) * 100)

with ProgressBar():
     missing_count_pct = missing_count.compute()

print(missing_count_pct)

col_drops = list(missing_count_pct[missing_count_pct >= 50].index)

with ProgressBar():
    df_cleaned_stage_1 = df.drop(col_drops, axis = 1)

fill_unknown_value = {
    'AGENCY': 'Unknown',
    'AGENCYNAME': 'Unknown',
    'EFFECTIVEDATE': 'Not Available',
    'CONTRACTINGOFFICER': 'Unknown',
}

with ProgressBar():
    df_cleaned_stage_2 = df_cleaned_stage_1.fillna(fill_unknown_value)

missing_values = df_cleaned_stage_2.isnull().sum()
missing_count = ((missing_values / df_cleaned_stage_2.index.size) * 100)

with ProgressBar():
     missing_count_pct = missing_count.compute()

print(missing_count_pct)

def func (x):
    try:
        return datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        return x

parse_last_modified = df_cleaned_stage_2['LASTMODIFIED'].apply(func, meta=datetime)
parse_award_date = df_cleaned_stage_2['AWARDDATE'].apply(func, meta=datetime)
parse_effective_date = df_cleaned_stage_2['EFFECTIVEDATE'].apply(func, meta=datetime)
parse_expiration_date = df_cleaned_stage_2['EXPIRATIONDATE'].apply(func, meta=datetime)
df_cleaned_stage_3 = df_cleaned_stage_2.drop(['LASTMODIFIED', 'AWARDDATE', 'EFFECTIVEDATE', 'EXPIRATIONDATE', 'DSC_LAST_MOD_DTTM'], axis=1)
df_cleaned_stage_4 = df_cleaned_stage_3.assign(LASTMODIFIED=parse_last_modified, AWARDDATE=parse_award_date, EFFECTIVEDATE=parse_effective_date, EXPIRATIONDATE=parse_expiration_date)

print(df_cleaned_stage_4.head())

bound_date = '2018-01'
condition = (df_cleaned_stage_4['AWARDDATE'] > bound_date) & (df_cleaned_stage_4['AMOUNT'] > 100000.0)
contracts_awarded_2018 = df_cleaned_stage_4[condition]
grouped_by_owner = contracts_awarded_2018.groupby('OWNER')
with ProgressBar():
    print(len(contracts_awarded_2018), "contracts awarded in 2018 with a value greater than $100,000")
    print(grouped_by_owner.agg({'AMOUNT': 'sum'}).compute())





