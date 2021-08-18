import os, couchdb2, pandas as pd
from core_functions import cfs, get_creds

pd.set_option('display.min_rows', 5) # How many to show
pd.set_option('display.max_rows', 50) # How many to show
pd.set_option('display.width', 200) # How far across the screen
pd.set_option('display.max_colwidth', 10) # Column width in px
pd.set_option('expand_frame_repr', True) # allows for the representation of dataframes to stretch across pages, wrapped over the full column vs row-wise

server = 'http://unique-foal.mrl.phy.ccds.io:5984/'
db = "series_norm"
db = "series_norm1" # Full data...might blow up your RAM, not sure yet ;)

# cfs(server, db)
couchdb_user, couchdb_pass = get_creds()

CFS = cfs(server,db, couchdb_user, couchdb_pass)
# pdb.set_trace()
# all_dicoms = CFS.requested_dicom_tags() # Took me ~4 min to load. RAM went from 6.44-13.4 (ended at 11.5) GB
# series_counts = CFS.series_counts_by_studyUID() # 11.4-11.6 GB
# series_desc_and_inst_count = CFS.series_desc_and_instance_counts() # 11.6- GB
CFS.add_private_tags_to_all_dicoms(all_dicoms_location="csvs/june/",path_to_combined_file="csvs/june/edited_with_mishkas_script")

dm = pd.read_csv('csvs/june/all_dicoms_w_private_tags.csv')
series_unique_tags_from_single_DICOM = CFS.series_tags(dm) # Found by finding highest frequency of unique tags among dicoms and then choosing one
# series_tags = CFS.series_unique_tags() # having trouble with this view and am now using function "series_tags"


len(dm['SeriesInstanceUID'].unique())


print("All Data:")
print(all_dicoms.head())
all_dicoms.to_csv(path_or_buf="csvs/june/all_dicoms.csv", 
                  sep=',', 
                  na_rep='', 
                  float_format=None, 
                  header=True, 
                  index=False, 
                  index_label=None, mode='w')
all_dicoms.iloc[0:100000,:].to_csv(path_or_buf="csvs/june/all_dicoms_100000.csv", 
                  sep=',', 
                  na_rep='', 
                  float_format=None, 
                  header=True, 
                  index=False, 
                  index_label=None, mode='w')
all_dicoms = pd.read_csv("csvs/june/all_dicoms.csv",
                    sep=','
)
all_dicoms = pd.read_csv("csvs/june/all_dicoms_w_private_tags.csv",
                    sep=','
)
# all_dicoms.loc[all_dicoms['SeriesDescription'].str.contains(')-(', regex=False),"SeriesDescription"] = "None"
print("Series Counts:")
print(series_counts.head())
series_counts.to_csv(path_or_buf="csvs/june/series_counts.csv", 
                  sep=',', 
                  na_rep='', 
                  float_format=None, 
                  header=True, 
                  index=False, 
                  index_label=None, mode='w')
series_counts = pd.read_csv("csvs/june/series_counts.csv",
                    sep=','
)

print("Instance Counts and Series Desc:")
print(series_desc_and_inst_count.head())
series_desc_and_inst_count.to_csv(path_or_buf="csvs/june/series_desc_and_inst_count.csv", 
                  sep=',', 
                  na_rep='', 
                  float_format=None, 
                  header=True, 
                  index=False, 
                  index_label=None, mode='w')
series_desc_and_inst_count.loc[series_desc_and_inst_count['SeriesDescription'].str.contains(')-(', regex=False),"SeriesDescription"] = "None"
print("Series Tags:")
print(series_unique_tags_from_single_DICOM.head())
series_unique_tags_from_single_DICOM.to_csv(path_or_buf="csvs/june/series_tags.csv", 
                  sep=',', 
                  na_rep='', 
                  float_format=None, 
                  header=True, 
                  index=False, 
                  index_label=None, mode='w')



# Exploring...not meant to be saved
ad = all_dicoms
ad.columns
ad[ad['InversionTime'] != 'Not Found'][['InversionTime', 'EchoTrainLength']]
list(ad[ad['InversionTime'] != 'Not Found']['fileNamePath'])[0]
ad[ad['EchoTrainLength'] != 'Not Found'][['InversionTime', 'EchoTrainLength']]
ad.head(100000)[['InversionTime', 'EchoTrainLength']]
len(ad['StudyInstanceUID'].unique())
patientIDs_from_all_dicoms = len(ad['PatientID'].unique())
len(ad['SeriesInstanceUID'].unique())
len(ad.iloc[0:100000]['PatientID'].unique())
len(ad['StudyInstanceUID'].unique())

series_counts_from_ad = ad.groupby("StudyInstanceUID").agg({'SeriesInstanceUID':'nunique'}).reset_index()
series_counts_from_ad.head()
series_counts_from_ad.to_csv('csvs/june/series_counts.csv',index=None)
series_counts_from_ad['SeriesInstanceUID'].sum()
series_counts_from_ad[series_counts_from_ad['StudyInstanceUID'] == '1.2.124.113532.80.22200.7094.20191002.121700.271895325']



st = pd.read_csv('csvs/june/series_tags.csv')
st = series_unique_tags_from_single_DICOM # alias
st.columns
st[st['InversionTime'] != 'Not Found'][['InversionTime', 'EchoTrainLength']]
st[st['EchoTrainLength'] != 'Not Found'][['InversionTime', 'EchoTrainLength']]

len(st['SeriesInstanceUID'].unique())
st.head()
st.shape
st.drop_duplicates().shape
st.iloc[0:10]
patientIDs_from_series_tags = len(st['PatientID'].unique())
sum(st.duplicated().values)
sum(st.drop(columns='ImageOrientationPatient').duplicated().values)

st_i = st.set_index('SeriesInstanceUID')
sum(st_i.duplicated().values)
sum(st_i.drop(columns='ImageOrientationPatient').duplicated().values)
# aren't multiple instance from same series...make sure

st.head()
st.dtypes

st.PatientID
st[st['PatientID'] == 12130837]
st[(st['PatientID'] == 10570190) & (st['SeriesDescription'] == 'AX T2 HASTE')]

ad[(ad['PatientID'] == 10570190) & (ad['SeriesDescription'] == 'AX T2 HASTE')]

l = ['1.3.12.2.1107.5.2.51.182932.30000020101608502312800000151','1.3.12.2.1107.5.2.51.182932.30000020101608502312800000329']
ad[ad['SeriesInstanceUID'].isin(l)].to_csv('csvs/june/temp.csv')
temp = ad[ad['SeriesInstanceUID'].isin(l)]

temp[temp['SeriesInstanceUID'] == '1.3.12.2.1107.5.2.51.182932.30000020101608502312800000151'].shape
temp[temp['SeriesInstanceUID'] == '1.3.12.2.1107.5.2.51.182932.30000020101608502312800000329'].shape



st.dtypes

st[st['SeriesInstanceUID'] == '1.2.276.0.45.1.7.3.83916715522378.20110409560500012.31692']
st.groupby('SeriesInstanceUID')['PatientID'].filter(lambda g: g.count() > 1)
st.groupby('PatientID')['SequenceName'].filter(lambda g: g.count() > 1).reset_index()


st.shape
len(st['SeriesInstanceUID'].unique())

series_to_investigate = ['1.2.276.0.45.1.7.3.83916715522378.20110412294700030.19668','1.2.276.0.45.1.7.3.83916715522378.20110412563600080.95763']
st[st.SeriesInstanceUID.isin(series_to_investigate)]


sc = series_counts
sc = pd.read_csv('csvs/june/series_counts.csv')
sc.head()
len(sc['StudyUID'].unique())
sc['SeriesCount'].sum()
sc.shape
sc[sc['StudyUID'] == '1.2.124.113532.80.22200.7094.20191002.121700.271895325']


# Compare sc to ad series count by study id
left = series_counts_from_ad
right = sc
merge = pd.merge(left,right, how='inner',left_on='StudyInstanceUID', right_on='StudyUID')
merge.head()
merge[merge['SeriesInstanceUID'] != merge['SeriesCount']]