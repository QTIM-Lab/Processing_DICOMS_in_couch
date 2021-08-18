import pydicom, sys, pdb, os, string, re
# pip install dicom or pydicom
from pydicom.data import get_testdata_file
from pydicom import dcmread
import pandas as pd, numpy as np
import multiprocessing, time
fpath = get_testdata_file("CT_small.dcm")

# pd.set_option('display.min_rows', 5) # How many to show
pd.set_option('display.max_rows', 50) # How many to show
pd.set_option('display.width', 200) # How far across the screen
pd.set_option('display.max_colwidth', 10) # Column width in px
pd.set_option('expand_frame_repr', True) # allows for the representation of dataframes to stretch across pages, wrapped over the full column vs row-wise

dicoms = [
        "/home_dir/JK/mnt/SeriesNormalizationMCRP/40162299/E17330600/1.2.840.113619.2.312.4120.14254601.14065.1610713757.133/MR.1.2.840.113619.2.312.4120.14254601.13415.1610713779.100.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/1175480/E16862548/1.2.392.200036.9123.100.12.12.22252.90201013131016088690251964/MR.1.2.392.200036.9123.100.12.12.22252.90201013135255097415729871.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/1175480/E16862548/1.2.392.200036.9123.100.12.12.22252.90201013131628089958241481/MR.1.2.392.200036.9123.100.12.12.22252.90201013133037092854731045.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/00203141/E17497993/1.3.12.2.1107.5.2.41.69565.30000020110319572670700004474/SRe.1.3.12.2.1107.5.2.41.69565.30000020110319572670700004503.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/00447321/E17522096/1.3.12.2.1107.5.2.41.69565.30000020110319572670700001434/SRe.1.3.12.2.1107.5.2.41.69565.30000020110319572670700001459.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/00908460/E17347683/1.3.12.2.1107.5.2.41.69565.30000020110319572670700000636/SRe.1.3.12.2.1107.5.2.41.69565.30000020110319572670700000659.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/1175480/E16862548/1.2.392.200036.9123.100.12.12.22252.90201013131015088688889787/MR.1.2.392.200036.9123.100.12.12.22252.90201013131106088861492683.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/1528184/E16452256/1.2.392.200036.9123.100.12.12.22252.90210209154628414348308243/MR.1.2.392.200036.9123.100.12.12.22252.90210209163244423813452088.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/3332344/E17134087/1.3.12.2.1107.5.2.43.167009.2020100416341770716921488.0.0.0/MR.1.3.12.2.1107.5.2.43.167009.2020100416341816286921505.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/3332344/E17134087/1.3.12.2.1107.5.2.43.167009.2020100416305292834519928.0.0.0/MR.1.3.12.2.1107.5.2.43.167009.202010041633269156320549.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/20097507/E16942169/1.3.12.2.1107.5.2.36.40168.2021012110282074519524686.0.0.0/MR.1.3.12.2.1107.5.2.36.40168.2021012110282347055524867.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/26303339/E17137309/1.3.12.2.1107.5.2.36.40291.2020101315063630311024751.0.0.0/MR.1.3.12.2.1107.5.2.36.40291.2020101315063625119124750.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/26303339/E17137309/1.3.12.2.1107.5.2.36.40291.2020101315051384634324649.0.0.0/MR.1.3.12.2.1107.5.2.36.40291.20201013150540722824741.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/41324419/E17179902/1.3.12.2.1107.5.2.36.40291.2020100807114214639557319.0.0.0/MR.1.3.12.2.1107.5.2.36.40291.2020100807151383698759658.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/41324419/E17179902/1.3.12.2.1107.5.2.36.40291.2020100807114214639657320.0.0.0/MR.1.3.12.2.1107.5.2.36.40291.2020100807151393979959696.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/23992498/E16898511/1.2.840.113619.2.80.0.5682.1601563957.1.13.2/MR.1.2.840.113619.2.80.0.5682.1601563957.19.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/01617612/E16987773/1.3.12.2.1107.5.2.19.45306.2021011111101851354849202.0.0.0/MR.1.3.12.2.1107.5.2.19.45306.202101111113388812554466.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/20097507/E16942169/1.3.12.2.1107.5.2.36.40168.2021012110282074519524686.0.0.0/MR.1.3.12.2.1107.5.2.36.40168.2021012110282347055524867.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/2751554/E17361897/1.2.840.113619.2.5.19231919171116054435021605443502805000/MR.1.2.840.113619.2.311.100196653089223718141359203512546696271.dcm",
        # "/home_dir/JK/mnt/SeriesNormalizationMCRP/01220044/E17038400/1.3.12.2.1107.5.2.41.169571.202010041023578162338990.0.0.0/MR.1.3.12.2.1107.5.2.41.169571.2020100410241583705439340.dcm",
        ]


# files = pd.read_csv("private_tags_0_11.csv")
# files = pd.read_csv("csvs/june/edited_with_mishkas_script/private_tags_1000000_1250000.csv")
# dicoms from csv: 

# files = pd.read_csv("csvs/apr/all_dicoms_100000.csv")
files = pd.read_csv("csvs/apr/all_dicoms.csv")

# pdb.set_trace()
# files = files[files["fileNamePath"].str.slice(0,5) == '../JK']

# files[files['fileNamePath'].str.len() < 100].to_csv("deleteme.csv", header=True, index=None)
# dicoms = [i.replace("../","/home_dir/") for i in files['fileNamePath']]


dicoms = [i.replace("../","/home_dir/") for i in files[~pd.isna(files['fileNamePath'])]['fileNamePath']]


# # print("done reading")
# pdb.set_trace()

# files.loc[files['fileNamePath'] == "../JK/mnt/SeriesNormalizationMCRP/4115931/E17154036/1.2.276.0.45.1.7.3.83916715583278.20100909260300023.25318/PSg.1.2.276.0.45.1.7.4.83916715583278.20100909260300024.25318.dcm"]

tag_key_original = {
    'EchoTime':'00180081',
    'InversionTime':'00180091',
    'EchoTrainLength':'00180082',
    'RepetitionTime':'00180080',
    'TriggerTime':'00181060',
    'SequenceVariant':'00180021',
    'ScanOptions':'00180022',
    'ScanningSequence':'00180020',
    'MRAcquisitionType':'00180023',
    'ImageType':'00180008',
    'PixelSpacing':'00280030',
    'SliceThickness':'00180050',
    'PhotometricInterpretation':'00280004',
    'ContrastBolusAgent':'00180010',
    'Modality':'00180060',
    'SeriesDescription':'0008103E'
}

tag_key = {
    'StudyInstanceUID':'0020000D',
    'SeriesInstanceUID':'0020000E',
    'SOPInstanceUID':'00080018',
    'PatientID':'00100020', #MRN
    'AccessionNumber':'00080050',
    'SequenceName':'00180024',
    'ImageComments':'00204000',
    'ProtocolName':'00181030',
    'ImagesInAcquisition':'00201002',
    'EchoTime':'00180081',
    'InversionTime':'00180082',
    'EchoTrainLength':'00180091',
    'RepetitionTime':'00180080',
    'TriggerTime':'00181060',
    'SequenceVariant':'00180021',
    'ScanOptions':'00180022',
    'ScanningSequence':'00180020',
    'MRAcquisitionType':'00180023',
    'ImageType':'00180008',
    'ImageOrientationPatient':'00200037',
    'FlipAngle': '00181314',
    'DiffusionBValue': '00189087',
    'SiemensBValue': '0019100C',
    'GEBValue': '0051100B',
    'SlopInt6-9': '00431039',
    'PulseSeqName': '0019109C',
    'InternalPulseSeqName': '0019109E',
    'FunctionalProcessingName': '00511002',
    'GEFunctoolsParams': '00511006',
    'CSA Series Header Info':'00291020',
    'Acq recon record checksum':'00211019',
    'PixelSpacing':'00280030',
    'SliceThickness':'00180050',
    'PhotometricInterpretation':'00280004',
    'ContrastBolusAgent':'00180010',
    'Modality':'00180060',
    'SeriesDescription':'0008103E'
}

# pdb.set_trace()
# Concat DFs:
def stich_dfs_together(location="csvs/apr/merge_these_csvs"):
    files = os.listdir(location)
    # files = ['private_tags_0_200000.csv','private_tags_200000_400000.csv','private_tags_400000_600000.csv','private_tags_600000_800000.csv','private_tags_800000_1000000.csv','private_tags_1000000_1200000.csv','private_tags_1400000_1501855.csv']
    total_rows = 0
    csv_name = os.path.join(location, "combined.csv")
    with open(csv_name, "w") as file:
        file.write("fileNamePath|00511006|00291020|0019109E\n")
    for file in files:
        print(file,"\n")
        # pdb.set_trace()
        file = pd.read_csv(os.path.join(location,file), sep="|")
        total_rows += file.shape[0]
        file.to_csv(csv_name, mode='a', sep="|", header=None, index=None)
    print(total_rows)
    
def test_combined_read(location="csvs/june/edited_with_mishkas_script"):
    file = pd.read_csv(os.path.join(location,"combined.csv"), sep="|")
    print(file.shape)


def pre_format_find_and_replace(s, tag):
    # print(tag)
    printable = set(string.printable)
    # pdb.set_trace()
    try:
        if s == '':
            S = "None"
        # pdb.set_trace()
        elif type(s) is pydicom.valuerep.IS:
            S = s.__str__()
        else:
            # pdb.set_trace()
            for i in s:
                if i not in printable:
                    s = s.replace(i," ")
            if s.find("\r") != -1:
                s = s.replace("\r"," ")
            if s.find("\n") != -1:
                s = s.replace("\n"," ")
            if s.find("|") != -1:
                s = s.replace("|"," ")
            S = s
            if tag == "00511006":
                S = GE_sequence_ID(s)
                # pdb.set_trace()
            elif tag == "00291020":
                S = siemens_sequence_ID(s)
                # pdb.set_trace()
            elif tag == "0019109E":
                S = GE_sequence_ID(s) # Essentially take this as is for now
                # pdb.set_trace()
            else:
                raise Exception("tag not recognized")
    except TypeError:
        pdb.set_trace()

    return S

def GE_sequence_ID(s):
    # pdb.set_trace()
    S = s
    return S


def siemens_sequence_ID(s):
    # pdb.set_trace()
    """
    Mishka's parser and filter for private GE\Seimens dicom tags
    """
    # siemens_tag = open(file) # file like object or str
    # siemens_text = siemens_tag.read()
    if s.find("\n") != -1:
        s = s.replace("\n"," ")
    siemens_text = s
    if siemens_text.find('tSequenceFileName') != -1:
        # start_idx = siemens_text.find('tSequenceFileName')
        # start_plus = siemens_text[start_idx:start_idx+100000].find('SiemensSeq')
        siemens_idx = siemens_text.find("SiemensSeq")
        if siemens_idx != -1:
            # Finding all occurrences of substring
            inilist = [m.start() for m in re.finditer(r"SiemensSeq%\\", siemens_text)]
            longest = ''
            for start in inilist:
                if len(siemens_text[start:start+30]) > len(longest):
                    # pdb.set_trace()
                    longest = siemens_text[start:start+30]
                # print("{}".format(siemens_text[start-15:start+30])) # debug
            
            # seqname = np.char.split(np.char.split(siemens_text[siemens_idx:siemens_idx+100], '""').tolist()[0],'\\').tolist()[1]
            seqname = np.char.split(np.char.split(longest, '""').tolist()[0],'\\').tolist()[1]
            # pdb.set_trace()
        elif siemens_text.find("CustomerSeq") != -1:
            siemens_idx = siemens_text.find("CustomerSeq")
            # Finding all occurrences of substring
            inilist = [m.start() for m in re.finditer(r"CustomerSeq%\\", siemens_text)]
            longest = ''
            for start in inilist:
                if len(siemens_text[start:start+30]) > len(longest):
                    longest = siemens_text[start:start+30]
            seqname = np.char.split(np.char.split(longest, '""').tolist()[0],'\\').tolist()[1]
            # pdb.set_trace()
        else:
            raise Exception("Couldn\'t find tSequenceFileName or CustomerSeq")

    else:
        # pdb.set_trace()
        seqname = siemens_text
    s = seqname
    return s

# dicoms_with_private_tags
def find_private_tags(range=(0,100000)):
    n = 0 # loop count
    with open("private_tags_{}_{}.csv".format(range[0],range[1]), "w") as file:
        file.write("fileNamePath|00511006|00291020|0019109E\n")
    for dicom_path in dicoms[range[0]:range[1]]:
        # print("Loading dicom: ", dicom_path.split('/')[-1], "and checking for many tags:")
        n += 1
        ds = dcmread(dicom_path)
        private_tags = [tag_key['GEFunctoolsParams'],tag_key['CSA Series Header Info'], tag_key['InternalPulseSeqName']]
        # temp_df = pd.DataFrame({'fileNamePath':[None],
        #                         private_tags[0]:[None],
        #                         private_tags[1]:[None]})
        # if ds.get(int("0x"+private_tags[2],16)) != None:
        # pdb.set_trace()
        if ds.get(int("0x"+private_tags[0],16)) != None or ds.get(int("0x"+private_tags[1],16)) != None or ds.get(int("0x"+private_tags[2],16)) != None:
            # ptag = private_tags[0] if ds.get(int("0x"+private_tags[1],16)) == None else private_tags[1]
            # temp_df['fileNamePath'] = dicom_path
            # temp_df[ptag] = ds.get(int("0x"+ptag,16)).value
            # dwpt = pd.concat((dwpt,temp_df), axis=0)
            
            # Mishka func...to parse tags...
            # siemens_sequence_ID()
            tag1 = "None" if ds.get(int("0x"+private_tags[0],16)) == None else ds.get(int("0x"+private_tags[0],16)).value
            tag2 = "None" if ds.get(int("0x"+private_tags[1],16)) == None else ds.get(int("0x"+private_tags[1],16)).value
            tag3 = "None" if ds.get(int("0x"+private_tags[2],16)) == None else ds.get(int("0x"+private_tags[2],16)).value
            tag1 = pre_format_find_and_replace(tag1, private_tags[0])
            tag2 = pre_format_find_and_replace(tag2, private_tags[1])
            tag3 = pre_format_find_and_replace(tag3, private_tags[2])
            # pdb.set_trace()
            # experimental
            frame = {'fileNamePath':pd.Series(dicom_path),'00511006':pd.Series(tag1),'00291020':pd.Series(tag2),'0019109E':pd.Series(tag3)}
            file = pd.DataFrame(frame)
            file[['fileNamePath','00511006','00291020','0019109E']].to_csv("csvs/apr/merge_these_csvs/private_tags_{}_{}.csv".format(range[0],range[1]), mode="a", header=False, sep="|", index=None)
            # pdb.set_trace()
            # experimental

            # original
            # with open("private_tags_{}_{}.csv".format(range[0],range[1]), "a") as file:
            #     file.write("{}|{}|{}\n".format(dicom_path,tag1,tag2))
            # original
        if n % 1000 == 0:
            print("batch:{}-{}, {}% complete".format(range[0], range[1], float(n)/(range[1]-range[0])*100))

        # if n == 3000:
        #     pdb.set_trace()
        # Keys
        # for key in tag_key.keys():
            # try:
            #     t = pydicom.datadict.tag_for_keyword(key)
            #     print("{}: {}".format(key, ds[t]))
            # except:
            #     print("{}: Not found".format(key))


def multiprocessing_find_private_tags():
    global dicoms
    starttime = time.time()
    processes = []
    process_count=40
    # dicoms = dicoms[0:14783]
    chunks = len(dicoms)/process_count
    remainder = len(dicoms) % process_count
    for i in range(0,process_count):
        start_images = i * chunks
        end_images = (i+1) * chunks
        # pdb.set_trace()
        print("{} - {}".format(start_images,end_images))
        p = multiprocessing.Process(target=find_private_tags, args=((start_images,end_images),))
        processes.append(p)
        p.start()
    if remainder != 0:
        start_images = process_count * chunks
        end_images = process_count * chunks + remainder
        print("{} - {}".format(start_images,end_images))
        p = multiprocessing.Process(target=find_private_tags, args=((start_images,end_images),))
        processes.append(p)
        p.start()

        
    for process in processes:
        process.join() # means wait for this to complete
        
    print('Time taken = {} seconds'.format(time.time() - starttime))



if __name__ == "__main__":
    os.chmod("pydicom_attempt.py",0x777)
    if sys.argv[1] == "combine":
        stich_dfs_together()
        test_combined_read()
    elif sys.argv[1] == "multiprocess":
        multiprocessing_find_private_tags()
    else:
        range_low=sys.argv[1]
        range_high=sys.argv[2]
        print("{}, {}".format(range_low,range_high))
        find_private_tags(range=(int(range_low),int(range_high)))





# sudo docker run --rm -it --name=ben_wks_python2_worker_1 -v /home/ben.bearce/:/home_dir -v /home/jayashree.kalpathy:/home_dir/JK ben_wks_python2 bash
# sudo docker run --rm -it --name=ben_wks_python2_worker_2 -v /home/ben.bearce/:/home_dir -v /home/jayashree.kalpathy:/home_dir/JK ben_wks_python2 bash
# sudo docker run --rm -it --name=ben_wks_python2_worker_3 -v /home/ben.bearce/:/home_dir -v /home/jayashree.kalpathy:/home_dir/JK ben_wks_python2 bash
# sudo docker run --rm -it --name=ben_wks_python2_worker_4 -v /home/ben.bearce/:/home_dir -v /home/jayashree.kalpathy:/home_dir/JK ben_wks_python2 bash
# sudo docker run --rm -it --name=ben_wks_python2_worker_5 -v /home/ben.bearce/:/home_dir -v /home/jayashree.kalpathy:/home_dir/JK ben_wks_python2 bash
# 750000 - 1253140,
# sudo docker run --rm -it --name=ben_wks_python2_worker_6 -v /home/ben.bearce/:/home_dir -v /home/jayashree.kalpathy:/home_dir/JK ben_wks_python2 bash"