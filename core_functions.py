import os, couchdb2, pydicom, pandas as pd, pdb, numpy as np
import getpass
from dotenv import load_dotenv


def get_creds():
    try:
        load_dotenv()
        couchdb_user = os.getenv('couchdb_user')
        couchdb_pass = os.getenv('couchdb_pass')
    except:
        has_env_file = input("Do you have a .env file with couchdb credentials? (y/n) ")
        if has_env_file == 'y' or has_env_file == 'Y':
            load_dotenv()
            couchdb_user = os.getenv('couchdb_user')
            couchdb_pass = os.getenv('couchdb_pass')
            try:
                len(couchdb_user)
                len(couchdb_pass)
            except TypeError:
                print("""
                    Check that you have a '.env' file with couchdb_user and couchdb_pass variables.
                    You can always enter a username\password.
                    Ex:
                    ```
                    couchdb_user=###...
                    couchdb_pass=###...
                    ```
                """)
                get_creds()
        elif has_env_file == 'n' or has_env_file == 'N':
            couchdb_user = getpass.getpass(prompt='Couchdb User: ', stream=None)
            couchdb_pass = getpass.getpass(prompt='Couchdb Password: ', stream=None)
        else:
            print("a (y/n) or (Y/N) is required...")
            get_creds()
    return couchdb_user, couchdb_pass




class cfs(object):
    def __init__(self, server, db, couchdb_user, couchdb_pass):
        self.server = server = couchdb2.Server(href='http://unique-foal.mrl.phy.ccds.io:5984/', 
                               username=couchdb_user, 
                               password=couchdb_pass,
                               use_session=True)
        self.db = couchdb2.Database(self.server, db, check=True)

    def requested_dicom_tags(self):
        """
        Query all loaded DICOMs and their headers
        """
            
        result = self.db.view('views', 'requested_dicom_tags', include_docs=False)
        keys = []
        values = []
        for row in result:
            id, key, value, doc = row
            keys.append(key); values.append(value)
        tag_name = [
            'StudyInstanceUID',
            'SeriesInstanceUID',
            'SOPInstanceUID',
            'PatientID',
            'AccessionNumber',
            'SequenceName',
            'ImageComments',
            'ProtocolName',
            'ImagesInAcquisition',
            'EchoTime',
            'InversionTime',
            'EchoTrainLength',
            'RepetitionTime',
            'TriggerTime',
            'SequenceVariant',
            'ScanOptions',
            'ScanningSequence',
            'MRAcquisitionType',
            'ImageType',
            'ImageOrientationPatient',
            'FlipAngle',
            'DiffusionBValue',
            'SiemensBValue',
            'GEBValue',
            'SlopInt6-9',
            'PulseSeqName',
            'FunctionalProcessingName',
            'GEFunctoolsParams',
            'CSA Series Header Info',
            'Acq recon record checksum',
            'PixelSpacing',
            'SliceThickness',
            'PhotometricInterpretation',
            'ContrastBolusAgent',
            'Modality',
            'SeriesDescription',
        ]
        pdb.set_trace()
        # tags = [pydicom.tag.Tag(tag) for tag in tag_name]
        # tags = [str(tag)[1:-1].replace(', ','') for tag in tags]
        # tag_and_name = dict(zip(tag_name, tags))

        header = ["fileNamePath"]+list(tag_name)
        df = pd.DataFrame(values, columns=header)
        df.loc[df['fileNamePath'].str.slice(0,5) != '../JK','fileNamePath'] = df[df['fileNamePath'].str.slice(0,5) != '../JK']["fileNamePath"].str.replace('./JK/mnt','../JK/mnt')
        df.loc[df['SeriesDescription'].str.contains(')-(', regex=False),"SeriesDescription"] = "None"
        # df.loc[df['SeriesDescription'].str.contains('None', regex=False),"SeriesDescription"]

        # pdb.set_trace()
        return df

    def series_counts_by_studyUID(self):
        """
        Get a list of series counts by study UID
        """
        result = self.db.view('views', 
                        'series_counts_by_studyUID', 
                        reduce=True,
                        group=True,
                        group_level=1,
                        include_docs=False)

        keys = []
        values = []
        for row in result:
            id, key, value, doc = row
            keys.append(key[0]); values.append(value)

        header = ["StudyUID","SeriesCount"]
        data = zip(keys,values)
        df = pd.DataFrame(data, columns=header)
        return df

    def series_desc_and_instance_counts(self):
        """
        Query all loaded DICOMs and their headers
        """
            
        result = self.db.view('views', 
                        'series_desc_and_instance_counts',
                        reduce=True,
                        group=True,
                        group_level=2,
                        include_docs=False)

        keys = []
        values = []
        for row in result:
            id, key, value, doc = row
            keys.append(key); values.append(value)

        values = [[v for v in value.values()] for value in values]
        # pdb.set_trace()
        data = zip(keys,values)
        data = [i[0]+i[1] for i in data]
        header = ["StudyUID","SeriesUID","SOPCount","SeriesDescription"]
        df = pd.DataFrame(data, columns=header)

        return df


    def series_unique_tags(self):
        """
        Query all loaded DICOMs and their headers
        """
            
        result = db.view('views', 
                        'series_desc_and_tags_from_instances',
                        reduce=True,
                        group=True,
                        group_level=2,
                        include_docs=False)

        keys = []
        values = []
        pdb.set_trace()
        for row in result:
            id, key, value, doc = row
            keys.append(key); values.append(value)
        tag_name = [
            'StudyInstanceUID',
            'SeriesInstanceUID',
            'SOPInstanceUID',
            'PatientID',
            'AccessionNumber',
            'SequenceName',
            'ImageComments',
            'ProtocolName',
            'ImagesInAcquisition',
            'EchoTime',
            'InversionTime',
            'EchoTrainLength',
            'RepetitionTime',
            'TriggerTime',
            'SequenceVariant',
            'ScanOptions',
            'ScanningSequence',
            'MRAcquisitionType',
            'ImageType',
            'ImageOrientationPatient',
            'PixelSpacing',
            'SliceThickness',
            'PhotometricInterpretation',
            'ContrastBolusAgent',
            'Modality',
            'SeriesDescription',
        ]
        tags = [pydicom.tag.Tag(tag) for tag in tag_name]
        tags = [str(tag)[1:-1].replace(', ','') for tag in tags]
        tag_and_name = dict(zip(tag_name, tags))
        header = list(tag_name)
        values = [[v for v in value.values()][0] for value in values]
        # data = zip(keys,values)
        # data = [i[0]+i[1] for i in data]
        # header = ["StudyUID","SeriesUID","SOPCount","SeriesDescription"]
        data = values
        df = pd.DataFrame(data, columns=header)
        df = df.drop(columns='SOPInstanceUID')
        

        return df

    # python only func
    def series_tags(self, df):
        # pdb.set_trace()

        # Get distinct tag sets by series
        unique_tags = ['SeriesInstanceUID',
                    'SOPInstanceUID', 
                    'PatientID', 
                    'AccessionNumber', 
                    'SequenceName', 
                    'ImageComments', 
                    'ProtocolName', 
                    'ImagesInAcquisition', 
                    'EchoTime', 
                    'InversionTime', 
                    'EchoTrainLength', 
                    'RepetitionTime', 
                    'TriggerTime', 
                    'SequenceVariant', 
                    'ScanOptions', 
                    'ScanningSequence', 
                    'MRAcquisitionType', 
                    'ImageType', 
                    'ImageOrientationPatient', 
                    # Start new tags
                    'FlipAngle',
                    'DiffusionBValue',
                    'SiemensBValue',
                    'GEBValue',
                    'SlopInt6-9',
                    'PulseSeqName',
                    'FunctionalProcessingName',

                    'GEFunctoolsParams',
                    'CSA Series Header Info',

                    'Acq recon record checksum',
                    # End new tags
                    'PixelSpacing', 
                    'SliceThickness', 
                    'PhotometricInterpretation', 
                    'ContrastBolusAgent', 
                    'Modality', 
                    'SeriesDescription']
        pdb.set_trace()
        all_dicoms_copy = df[unique_tags].copy(deep=True)
        tags_list = []
        def mash(row):
            row_values = []
            row_dict = dict(zip(row.index,row.values))
            for k in row_dict.keys():
                try:
                    if (k == "SOPInstanceUID"):
                        pass
                    elif pd.isna(row_dict['SeriesDescription']):
                        pass
                    elif (k == "SeriesDescription" and row_dict['SeriesDescription'].find(")-(") != -1):
                    # elif (k == "SeriesDescription" and row_dict['SeriesDescription'].find(")-(") != -1 and row_dict['SeriesInstanceUID'] == "1.2.840.113619.2.312.4120.14254601.13536.1610722606.306"):
                        # pdb.set_trace()
                        do_we_want_these = False
                        s = row_dict['SeriesDescription']
                        first_slash = s.find("/")
                        second_slash = s.find("/", first_slash+1)
                        third_slash = s.find("/", second_slash+1)
                        fourth_slash = s.find("/", third_slash+1)
                        second_parenthesis = s.find("(",1)
                        string = s[0:second_slash]+"/#)-"+s[second_parenthesis:fourth_slash]+"/#)"
                        if do_we_want_these:
                            row_values.append(string)
                        else:
                            row_values.append("")
                        # pdb.set_trace()
                    elif (isinstance(row_dict[k], list)):
                        temp_array_string = "["+",".join(row_dict[k])+"]"
                        row_values.append(temp_array_string)
                    else:
                        row_values.append(str(row_dict[k]))
                except AttributeError as e:
                    pdb.set_trace()
            try:
                # pdb.set_trace()
                unique_tag = '|'.join(row_values)
            except ValueError as e:
                print(":(")
                # pdb.set_trace()
            # pdb.set_trace()
            return unique_tag
        # pdb.set_trace()
        all_dicoms_copy['unique_tags'] = all_dicoms_copy[unique_tags].apply(mash, axis=1)
        # pdb.set_trace()
        
        # Count distinct tags sets by series
        all_dicoms_grouped = all_dicoms_copy.groupby(['SeriesInstanceUID','unique_tags'])
        # distinct_tag_set_counts = all_dicoms_grouped.agg({"SOPInstanceUID": "count"})
        distinct_tag_set_counts = all_dicoms_grouped['SOPInstanceUID'].count()
        distinct_tag_set_counts = distinct_tag_set_counts.reset_index()
        # distinct_tag_set_counts.index
        # distinct_tag_set_counts.head()
        # distinct_tag_set_counts[50:100]
        # distinct_tag_set_counts[distinct_tag_set_counts['SeriesInstanceUID'] == "1.2.276.0.45.1.7.3.79232739332376.20121015302300080.18204"]
        # for i in list(distinct_tag_set_counts[distinct_tag_set_counts['SeriesInstanceUID'] == "1.2.276.0.45.1.7.3.79232739332376.20121015302300080.18204"]['unique_tags']): print(i, '\n')
        # pdb.set_trace()

        # Find highest frequency tag set by series
        highest_freq_by_study = distinct_tag_set_counts.groupby(['SeriesInstanceUID'])['SOPInstanceUID'].max().reset_index()
        
        # Pair that tag set with series id
        highest_freq_by_study_join = highest_freq_by_study.merge(distinct_tag_set_counts, on=["SeriesInstanceUID","SOPInstanceUID"], how='left')
        ## Still could have unique tag sets with same frequency, especially at lower frequencies.
        ## So we take the first of all equal frequency sets of unique tags
        unique_tags_by_series = highest_freq_by_study_join.groupby(["SeriesInstanceUID","SOPInstanceUID"])['unique_tags'].first().reset_index()

        # Prepare output
        new_header = [i for i in unique_tags if i != "SOPInstanceUID"]
        result = unique_tags_by_series['unique_tags'].str.split("|", expand=True)
        result.columns = new_header
        # result[8600:8650]
        # result[result['SeriesInstanceUID'] == "1.2.840.113619.2.312.4120.14254601.13536.1610722606.306"]
        # len(new_header)

        # pdb.set_trace()
        return result

    def add_private_tags_to_all_dicoms(self, all_dicoms_location, path_to_combined_file):
        # read
        all_dicoms = pd.read_csv(os.path.join(all_dicoms_location,"all_dicoms.csv"),sep=',')
        # pdb.set_trace()
        combined_private_tags = pd.read_csv(os.path.join(path_to_combined_file,"combined.csv"), sep="|", engine='python')
        private_tags = {
            'GEFunctoolsParams': '00511006',
            'CSA Series Header Info':'00291020'
        }
        # sync columns for join and file path beginnings
        all_dicoms.rename(columns=private_tags, inplace=True)
        combined_private_tags['fileNamePath'] = combined_private_tags['fileNamePath'].str.replace('/home_dir/JK','../JK')
        # pdb.set_trace()
        # create full dataset
        merge = pd.merge(all_dicoms, combined_private_tags, how='left', on='fileNamePath', suffixes=('_a','_c'))
        # merge.head()
        # header for not printing filePathName to terminal
        header = ['00511006_a','00511006_c','00291020_a','00291020_c']
        # GE Tags Merge QA Questions
        # Any that all has that combined didn't?
        filter = ( (pd.isna(merge['00511006_c'])) | (merge['00511006_c'] == "None") ) & ( (merge['00511006_a'] != "Not Found") & (~pd.isna(merge['00511006_a'])) )
        merge[filter][header] # 336
        # Any where both all and combined had data?
        filter = ( (~pd.isna(merge['00511006_c'])) & (merge['00511006_c'] != "None") ) & ( (merge['00511006_a'] != "Not Found") & (~pd.isna(merge['00511006_a'])) )
        merge[filter][header] # 2514
        # Any where only combined had data?
        filter = ( (~pd.isna(merge['00511006_c'])) & (merge['00511006_c'] != "None") ) & ( (merge['00511006_a'] == "Not Found") | (pd.isna(merge['00511006_a'])) )
        merge[filter][header] # 0
        # Any where either combined or all had data
        filter = ( (~pd.isna(merge['00511006_c'])) & (merge['00511006_c'] != "None") ) | ( (merge['00511006_a'] != "Not Found") & (~pd.isna(merge['00511006_a'])) )
        merge[filter][header] # 2850
        # Any where all and combined had no data
        filter = ( (pd.isna(merge['00511006_c'])) | (merge['00511006_c'] == "None") ) & ( (merge['00511006_a'] == "Not Found") | (pd.isna(merge['00511006_a'])) )
        merge[filter][header] # 1431040
        
        # Siemens Tags Merge QA Questions
        # Any that all has that combined didn't?
        filter = ( (pd.isna(merge['00291020_c'])) | (merge['00291020_c'] == "None") ) & ( (merge['00291020_a'] != "Not Found") & (~pd.isna(merge['00291020_a'])) )
        merge[filter][header] # 416251
        # Any where both all and combined had data?
        filter = ( (~pd.isna(merge['00291020_c'])) & (merge['00291020_c'] != "None") ) & ( (merge['00291020_a'] != "Not Found") & (~pd.isna(merge['00291020_a'])) )
        merge[filter][header] # 243675
        # Any where only combined had data?
        filter = ( (~pd.isna(merge['00291020_c'])) & (merge['00291020_c'] != "None") ) & ( (merge['00291020_a'] == "Not Found") | (pd.isna(merge['00291020_a'])) )
        merge[filter][header] # 0
        # Any where either combined or all had data
        filter = ( (~pd.isna(merge['00291020_c'])) & (merge['00291020_c'] != "None") ) | ( (merge['00291020_a'] != "Not Found") & (~pd.isna(merge['00291020_a'])) )
        merge[filter][header] # 659926
        # Any where all and combined had no data
        filter = ( (pd.isna(merge['00291020_c'])) | (merge['00291020_c'] == "None") ) & ( (merge['00291020_a'] == "Not Found") | (pd.isna(merge['00291020_a'])) )
        merge[filter][header] # 773964

        
        # pdb.set_trace()
        # Decide whether to keep all_dicoms data or combineds data and when.
        total_rows = merge.shape[0] # 1433890
        merge['00511006'] = '' # final tag value
        merge['00291020'] = '' # final tag value
        
        # We have to take the rows where combined had data and all_dicoms didn't
        # Any where only combined had data?
        filter = ( (~pd.isna(merge['00511006_c'])) & (merge['00511006_c'] != "None") ) & ( (merge['00511006_a'] == "Not Found") | (pd.isna(merge['00511006_a'])) )
        merge.loc[filter]['00511006'] = merge[filter]['00511006_c'] # 0 
        filter = ( (~pd.isna(merge['00291020_c'])) & (merge['00291020_c'] != "None") ) & ( (merge['00291020_a'] == "Not Found") | (pd.isna(merge['00291020_a'])) )
        merge.loc[filter]['00291020'] = merge[filter]['00291020_c'] # 0 

        # Any that all has that combined didn't?
        filter = ( (pd.isna(merge['00511006_c'])) | (merge['00511006_c'] == "None") ) & ( (merge['00511006_a'] != "Not Found") & (~pd.isna(merge['00511006_a'])) )
        merge.loc[filter,'00511006'] = merge[filter]['00511006_a'] # 336        
        filter = ( (pd.isna(merge['00291020_c'])) | (merge['00291020_c'] == "None") ) & ( (merge['00291020_a'] != "Not Found") & (~pd.isna(merge['00291020_a'])) )
        merge.loc[filter,'00291020'] = merge[filter]['00291020_a'] # 416251

        # Any where both all and combined had data?
        filter = ( (~pd.isna(merge['00511006_c'])) & (merge['00511006_c'] != "None") ) & ( (merge['00511006_a'] != "Not Found") & (~pd.isna(merge['00511006_a'])) )
        merge.loc[filter,'00511006'] = merge[filter]['00291020_c'] # 2514
        filter = ( (~pd.isna(merge['00291020_c'])) & (merge['00291020_c'] != "None") ) & ( (merge['00291020_a'] != "Not Found") & (~pd.isna(merge['00291020_a'])) )
        merge.loc[filter,'00291020'] =  merge[filter]['00291020_c'] # 243675

        # Any where all and combined had no data
        header = ['00511006','00291020']
        filter = merge['00511006'] == ''
        merge.loc[filter][header] # 1431040
        # Any where all and combined had no data
        filter = merge['00291020'] == ''
        merge.loc[filter][header] # 773964

        pdb.set_trace()
        merge.drop(['00511006_a', '00291020_a','00511006_c', '00291020_c'], inplace=True, axis=1)
        merge.rename(columns={'00511006':'GEFunctoolsParams','00291020':'CSA Series Header Info'}, inplace=True)
        merge.to_csv('csvs/june/all_dicoms_w_private_tags.csv')
