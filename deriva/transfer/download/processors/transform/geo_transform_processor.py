import os
import json
import importlib
import logging
import re
from copy import copy
from enum import Enum
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.transform.base_transform_processor import BaseTransformProcessor, \
    LOCAL_PATH_KEY, SOURCE_URL_KEY


# this processor transforms sequence data in RBK to excel file of GEO format
# https://www.ncbi.nlm.nih.gov/geo/info/seq.html
class GeoExportTransformProcessor(BaseTransformProcessor):
    def __init__(self, envars=None, **kwargs):
        super(GeoExportTransformProcessor, self).__init__(envars, **kwargs)
        self._create_input_output_paths()

    def process(self):
        logging.info("Transforming input file [%s] into output file [%s]" %
                     (self.input_abspath, self.output_abspath))
        # input_abspath: the json file containing raw data retrieved from DB
        # output_abspath: the excel file of GEO format
        geo = Export2GEO(self.input_abspath, self.output_abspath, self.parameters)
        geo.export_all()

        super(GeoExportTransformProcessor, self).process()
        return self.outputs


# used for setting background color
class Style(Enum):
    DEFAULT = 0
    INSTRUCTION = 1
    SECTION = 2
    FIELD = 3
    HEADER = 4
    PREFACE = 5


# handling excel operations
class Export2Excel(object):
    OPENPYXL = None

    def __init__(self, output_path):
        self.__name__ = 'export_excel'
        self.wb = None
        self.ws = None
        self._output_path_ = output_path
        self.sheet_name = 'METADATA'
        self.initialize_wb()

    def import_openpyxl(self):
        # locate library
        if self.OPENPYXL is None:
            try:
                self.OPENPYXL = importlib.import_module("openpyxl")
            except ImportError as e:
                raise DerivaDownloadConfigurationError("Unable to find required module. "
                                                       "Ensure that the Python package \"openpyxl\" is installed.", e)

    def initialize_wb(self):
        self.import_openpyxl()
        # Open an xlsx
        self.wb = self.OPENPYXL.Workbook()
        self.ws = self.wb.create_sheet(index=0, title=self.sheet_name)

        # remove the default sheet
        self.wb.remove(self.wb['Sheet'])

        fgColor0 = self.OPENPYXL.styles.colors.Color(indexed=10, type='indexed')
        fgColor1 = self.OPENPYXL.styles.colors.Color(indexed=42, type='indexed')
        bgColor1 = self.OPENPYXL.styles.colors.Color(indexed=27, type='indexed')
        fgColor2 = self.OPENPYXL.styles.colors.Color(indexed=12, type='indexed')
        fgColor3 = self.OPENPYXL.styles.colors.Color(indexed=13, type='indexed')
        bgColor3 = self.OPENPYXL.styles.colors.Color(indexed=34, type='indexed')

        self.font_red = self.OPENPYXL.styles.Font(name='Arial', b=True, color=fgColor0, sz=10.0)
        self.font_blue = self.OPENPYXL.styles.Font(name='Arial', b=True, color=fgColor2, sz=10.0)
        self.font_bond = self.OPENPYXL.styles.Font(name='Arial', b=True, sz=10.0)

        self.fill_green = self.OPENPYXL.styles.PatternFill('solid', fgColor=fgColor1, bgColor=bgColor1)
        self.fill_yellow = self.OPENPYXL.styles.PatternFill('solid', fgColor=fgColor3, bgColor=bgColor3)

        self.ws.column_dimensions['A'].width = 42.0
        self.ws.column_dimensions['B'].width = 15.0
        self.ws.column_dimensions['C'].width = 24.0
        self.ws.column_dimensions['D'].width = 15.0
        self.ws.column_dimensions['E'].width = 24.0
        self.ws.column_dimensions['F'].width = 24.0
        self.ws.column_dimensions['G'].width = 24.0
        self.ws.column_dimensions['H'].width = 24.0
        self.ws.column_dimensions['I'].width = 24.0
        self.ws.column_dimensions['J'].width = 24.0
        self.ws.column_dimensions['K'].width = 24.0
        self.ws.column_dimensions['L'].width = 24.0
        self.ws.column_dimensions['M'].width = 24.0
        self.ws.column_dimensions['N'].width = 24.0

    @staticmethod
    # row id and column id to excel cell location
    def cell_name(row_num, col_num):
        string = ""
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            string = chr(65 + remainder) + string
        return string + str(row_num)

    def insert_row(self, row_idx, row_amt):
        self.ws.insert_rows(idx=row_idx, amount=row_amt)

    def delete_row(self, idx, row_amt):
        self.ws.delete_rows(idx=idx, amount=row_amt)

    # writing value into cell with some style
    def write_cell(self, row_idx, col_idx, content, export_style=Style.DEFAULT):
        cell_loc = self.cell_name(row_idx, col_idx)
        cell = self.ws[cell_loc]
        cell.value = content
        if (export_style == Style.INSTRUCTION):
            cell.fill = self.fill_green
        elif (export_style == Style.HEADER):
            cell.fill = self.fill_yellow
            cell.font = self.font_blue
        elif (export_style == Style.FIELD):
            cell.font = self.font_blue
        elif (export_style == Style.SECTION):
            cell.font = self.font_red
        elif (export_style == Style.PREFACE):
            cell.fill = self.fill_green
            cell.font = self.font_bond
        else:
            pass

    def save_xlsx(self):
        self.wb.save(self._output_path_)

    def copy_cell(self, old_row_idx, old_col_idx, new_row_idx, new_col_idx, copy_style=True):
        cell = self.ws[self.cell_name(old_row_idx, old_col_idx)]
        new_cell = self.ws[self.cell_name(new_row_idx, new_col_idx)]
        new_cell.value = cell.value
        if cell.has_style and copy_style:
            new_cell.font = copy(cell.font)
            new_cell.border = copy(cell.border)
            new_cell.fill = copy(cell.fill)
            new_cell.number_format = copy(cell.number_format)
            new_cell.protection = copy(cell.protection)
            new_cell.alignment = copy(cell.alignment)
            new_cell.comment = copy(cell.comment)


class Export2GEO(object):

    def __init__(self, input_file, output_excel_path, parameters):
        self.__name__ = 'export_geo_template'
        self.input_file = input_file
        self.output_excel_path = output_excel_path
        self.parameters = parameters
        self.current_row_idx = 0
        self.header_row_idx = 0
        # all types of protocols
        self.protocol_type = ['Isolation_Protocol', 'Growth_Protocol', 'Treatment_Protocol', 'Extract_Protocol',
                              'Construction_Protocol', 'Label_Protocol', 'Hybridization_Protocol', 'Scan_Protocol']
        self.protocol_unique = {}
        self.protocol_list = {}
        self.other_item = ['Data_Processing', 'Reference_Genome']
        self.other_item_unique = {}
        self.other_item_list = {}
        # whether to filter study_file
        # note: ext should all be lower case as we lower case the filename before comparison
        self.study_file_filter = True
        self.study_file_ext_list = [".csv",".csv.gz",".txt.gz",".txt",".tsv",".tsv.gz",".xls",".xlsx",".mtx",".bw"]
        # whether to filter replicate_files
        # Removing .bai, .I1.fastq.gz from whitelist, GEO will ignore it as per Gervaise.
        # note: ext should all be lower case as we lower case the filename before comparison
        self.replicate_file_filter = True
        self.replicate_file_ext_list = [ ".bam","r1.fastq","r1.fastq.gz","r2.fastq","r2.fastq.gz" ] 
        self.initialize_data()

    def initialize_data(self):
        #print("initilaize_data: begin")
        with open(self.input_file) as input_file:
            data = json.load(input_file)
            self.study = None
            if data is None or len(data) == 0:
                logging.debug("No Data")
            elif len(data) > 1:
                logging.debug("Duplicate Data")
            elif len(data) == 1:
                # get parameters from json file
                self.study = data[0].get('Study')[0]
                self.pi = data[0].get('Principal_Investigator')[0]
                self.experiments = data[0].get('Experiment')
                self.experiment_settings = data[0].get('Experiment_Setting')
                self.replicates = data[0].get('Replicate')
                self.specimen = data[0].get('Specimen')
                self.files = data[0].get('File')
                self.study_files = data[0].get('Study_File')
                self.stage = data[0].get('Stage')
                self.tissue = data[0].get('Tissue')
                self.source_type = data[0].get('Source')
                self.specimen_cell_type = data[0].get('Specimen_Cell_Type')
                self.cell_type = data[0].get('Cell_Type')
                self.specimen_allele = data[0].get('Specimen_Allele')
                self.allele = data[0].get('Allele')


                # Adding sorting to files based on key ( Replicate-RID, FileName )
                # https://github.com/informatics-isi-edu/rbk-project/issues/615
                # the array can contain None, so the function needs to handle this case. 
                if self.replicates and len(self.replicates) > 1:
                    self.replicates.sort( key = (lambda x: x.get('RID') if x is not None else ''))
                if self.files and len(self.files) > 1:
                    self.files.sort( key = (lambda x: (x.get('Replicate_RID'), x.get('File_Name')) if x is not None else ('', '')))                    
                if self.study_files and len(self.study_files) > 1:
                    self.study_files.sort( key = (lambda x: x.get('File_Name') if x is not None else ''))
        #print("initilaize_data: end")


    def export_start(self):
        self.excel = Export2Excel(self.output_excel_path)

    def export_all(self):
        if self.study is not None:
            # export data for different sections such as series,samples etc.
            self.export_start()
            self.export_preface()
            self.export_series()
            self.export_sample_prepare()
            self.export_sample()
            self.export_protocol()
            self.export_data_processing()
            self.export_processed_datafiles()
            self.export_raw_datafiles()
            self.export_paired_end()
            self.export_finish()

    # export preface sections in GEO template excel
    # words may need update if GEO template update
    # https://www.ncbi.nlm.nih.gov/geo/info/seq.html#metadata
    def export_preface(self):
        #print("export_preface: start")
        self.current_row_idx = 1
        self.excel.write_cell(self.current_row_idx, 1, '# High-throughput sequencing metadata template (version 2.1).',
                              Style.PREFACE)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, '# All fields in this template must be completed.',
                              Style.PREFACE)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              '# Templates containing example data are found in the METADATA EXAMPLES spreadsheet tabs at the foot of this page.',
                              Style.PREFACE)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              '# Field names (in blue on this page) should not be edited. Hover over cells containing field names to view field content guidelines.',
                              Style.PREFACE)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, (
            '# Human data. If there are patient privacy concerns regarding making data fully public through GEO, '
            'please submit to NCBI''s dbGaP (http://www.ncbi.nlm.nih.gov/gap/) database. dbGaP has controlled access'
            'mechanisms and is an appropriate resource for hosting sensitive patient data.'), Style.PREFACE)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, '', Style.PREFACE)
        self.current_row_idx += 1

        for col in range(2, 13):
            for row in range(1, 7):
                self.excel.write_cell(row, col, '', Style.PREFACE)
        #print("export_preface: end")

    # export SERIES(study)
    def export_series(self):
        ##print("export_series: begin")
        self.excel.write_cell(self.current_row_idx, 1, 'SERIES', Style.SECTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, '# This section describes the overall experiment.',
                              Style.INSTRUCTION)
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'title', Style.FIELD)
        self.excel.write_cell(self.current_row_idx, 2, self.study.get('Title', ''))
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'summary', Style.FIELD)
        self.excel.write_cell(self.current_row_idx, 2, self.study.get('Summary', ''))
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'overall design', Style.FIELD)
        self.excel.write_cell(self.current_row_idx, 2, self.study.get('Overall_Design', ''))
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'contributor', Style.FIELD)
        self.excel.write_cell(self.current_row_idx, 2, self.pi.get('Full_Name', ''))
        self.current_row_idx += 1

        # one line for each study file
        if self.study_files and len(self.study_files) >= 1:
            for pf in self.study_files:
                # in a left join, any row could be null. Check each row. 
                if pf is None:
                    continue
                # check against the white list
                if self.study_file_filter:
                    valid_file = False
                    for str in self.study_file_ext_list:
                        if pf.get('File_Name') is not None and pf.get('File_Name').lower().endswith(str):
                            valid_file = True
                            break
                else:
                    valid_file = True

                if valid_file:
                    file_path = self.parameters.get("study_files_path_template", pf.get("File_Name", ""))
                    self.excel.write_cell(self.current_row_idx, 1, 'supplementary file', Style.FIELD)
                    self.excel.write_cell(self.current_row_idx, 2, file_path.format(**pf))
                    self.current_row_idx += 1
        else:
            self.excel.write_cell(self.current_row_idx, 2, '')
            logging.debug('No study file')
        #print("export_series: end")
        
    # prepare the data for sample export
    def export_sample_prepare(self):
        #print("export_sample_prepare: begin")        
        # check self.protocol unique
        for p in self.protocol_type:
            # protocols which are applicable to only a subset of Samples should be included
            # as additional columns of the SAMPLES section instead.
            # this dictionary stores if containing unique content for each protocol type or not
            # used to handle dynamic headers related to the protocols
            self.protocol_unique[p] = True
            # initialize list of items for each protocol type
            self.protocol_list[p] = []

        for i in self.other_item:
            self.other_item_unique[i] = True
            self.other_item_list[i] = []

        for e in self.experiments:
            if e is None or 'RID' not in e.keys():
                continue
            for p in self.protocol_type:
                if p not in e.keys():
                    continue
                elif len(self.protocol_list[p]) == 0 and e[p] is not None and e[p] != 'None':
                    # add first protocol for this protocol type
                    self.protocol_list[p].append(e[p])
                elif e[p] not in self.protocol_list[p] and e[p] is not None and e[p] != 'None':
                    # add other protocols for this protocol type
                    self.protocol_list[p].append(e[p])
                    # and this protocol type doesn't have unique value
                    self.protocol_unique[p] = False
            i = 'Data_Processing'
            if i in e.keys():
                # include experiment parameters in data processing part
                if len(self.other_item_list[i]) == 0 and e[i] is not None and e[i] != 'None':
                    self.other_item_list[i].append(e[i])
                elif e[i] not in self.other_item_list[i] and e[i] is not None and e[i] != 'None':
                    self.other_item_list[i].append(e[i])
                    self.other_item_unique[i] = False

        for e in self.experiment_settings:
            if e is None:
                continue
            i = 'Reference_Genome'
            if i in e.keys():
                if len(self.other_item_list[i]) == 0 and e[i] is not None and e[i] != 'None':
                    self.other_item_list[i].append(e[i])
                elif e[i] not in self.other_item_list[i] and e[i] is not None and e[i] != 'None':
                    self.other_item_list[i].append(e[i])
                    self.other_item_unique[i] = False
        #print("export_sample_prepare: end")
    
    # export data of all the samples in one study
    def export_sample(self):
        #print("export_sample: begin")
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'SAMPLES', Style.SECTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              ('# This section lists and describes each of the biological Samples under investgation,'
                               'as well as any protocols that are specific to individual Samples.'), Style.INSTRUCTION)
        # write some blank cells to make words of last cell showing expanded.
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              '# Additional "processed data file" or "raw file" columns may be included.',
                              Style.INSTRUCTION)
        # write some blank cells to make words of last cell showing expanded.
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.header_row_idx = self.current_row_idx + 1
        self.current_row_idx += 1

        # write SAMPLES
        # experiment by experiment
        for e in self.experiments:
            if e is None or 'RID' not in e.keys():
                continue
            for r in self.replicates:
                if r is None or 'Experiment_RID' not in r.keys() or 'RID' not in r.keys():
                    continue
                elif r['Experiment_RID'] == e['RID']:
                    # construct sample_name using internal_ID,Biological_Replicate_Number and Technical_Replicate_Number
                    sample_name = e.get('Internal_ID', '') + '_' + str(
                        r.get('Biological_Replicate_Number', '')) + '_' + str(
                        r.get('Technical_Replicate_Number', ''))
                    sample_title = e.get('Name', '') + '_' + str(r.get('Biological_Replicate_Number', '')) + '_' + str(
                        r.get('Technical_Replicate_Number', ''))
                    sample_organism = e.get('Species', '')
                    sample_molecule = e.get('Molecule_Type', '')
                    sample_description = r.get('Notes', '') # should use Specimen.Upload_Notes instead. This is being ignored.


                    # the characteristic should show as one column in sample section, if samples has value it
                    characteristic_list = ['Phenotype', 'Stage_ID', 'Stage_Detail', 'Genotype', 'Strain', 'Wild_Type',
                        'Cell_Line','Passage','Assay_Type','Sex']
                    # foreign_item_list = ['Tissue', 'Source', 'Specimen_Cell_Type', 'Cell_Type']
                    characteristic_exist = []
                    for s in self.specimen:
                        for c in characteristic_list:
                            characteristic = s.get(c, '')
                            if characteristic is not None and len(characteristic) > 0 and c not in characteristic_exist:
                                # get a list of existing characteristics
                                characteristic_exist.append(c)
                            else:
                                continue

                    # check specimen tissue for "source name"
                    #/Src:=left(Tissue)=(Vocabulary:Anatomy:ID)
                    if self.source_type:
                        source_name_exist = False
                        for f in self.source_type:
                            if f is not None:
                                source_name_exist = True
                                break
                    
                    # check other characteristics
                    if self.cell_type:
                        for f in self.cell_type:
                            if f is None:
                                continue
                            else:
                                characteristic_exist.append('Cell_Type')
                                break
                    if self.specimen_allele:
                        for f in self.specimen_allele:
                            if f is None:
                                continue
                            else:
                                characteristic_exist.append('Allele')
                                break

                    # consolidated stage column
                    if 'Stage_ID' in characteristic_exist and 'Stage_Detail' in characteristic_exist:
                        characteristic_exist.remove('Stage_ID')
                        characteristic_exist.remove('Stage_Detail')
                        characteristic_exist.append('Stage')
                    elif 'Stage_ID' in characteristic_exist:
                        characteristic_exist.remove('Stage_ID')
                        characteristic_exist.append('Stage')
                    elif 'Stage_Detail' in characteristic_exist:
                        characteristic_exist.remove('Stage_Detail')
                        characteristic_exist.append('Stage')

                    # -- start writing
                    local_col_idx = 1
                    self.current_row_idx += 1

                    self.excel.write_cell(self.header_row_idx, local_col_idx, 'Sample name', Style.HEADER)
                    self.excel.write_cell(self.current_row_idx, local_col_idx, sample_name)
                    local_col_idx += 1
                    self.excel.write_cell(self.header_row_idx, local_col_idx, 'title', Style.HEADER)
                    self.excel.write_cell(self.current_row_idx, local_col_idx, sample_title)
                    local_col_idx += 1


                    # handling source name
                    for s in self.specimen:
                        if s is None or 'REPLICATE_RID' not in s.keys():
                            continue
                        elif s['RID'] == r['Specimen_RID']:
                            # if source_name_exist, add a column
                            if source_name_exist:
                                source_type = ''
                                for p in self.tissue:
                                    for q in self.source_type:
                                        if p is None or q is None:
                                            continue
                                        elif s['RID'] == p['Specimen_RID'] and p['Tissue'] == q['ID']:
                                            source_type = source_type + q['Name'] + ','
                                        else:
                                            continue
                                characteristic = source_type[:-1] if len(source_type) > 0 else ''
                                self.excel.write_cell(self.header_row_idx, local_col_idx, 'source name', Style.HEADER)
                                self.excel.write_cell(self.current_row_idx, local_col_idx, characteristic)
                                local_col_idx += 1
                    
                    # handling organism
                    self.excel.write_cell(self.header_row_idx, local_col_idx, 'organism', Style.HEADER)
                    self.excel.write_cell(self.current_row_idx, local_col_idx, sample_organism)
                    local_col_idx += 1


                    for p in self.protocol_type:
                        # if different contents for protocol_type, need to add column for this protocol_type
                        # then different samples may has different content to write
                        if not self.protocol_unique[p]:
                            self.excel.write_cell(self.header_row_idx, local_col_idx, (p.replace('_', ' ').lower()),
                                                  Style.HEADER)
                            self.excel.write_cell(self.current_row_idx, local_col_idx, e[p])
                            local_col_idx += 1


                    for s in self.specimen:
                        if s is None or 'REPLICATE_RID' not in s.keys():
                            continue
                        elif s['RID'] == r['Specimen_RID']:
                            for c in characteristic_exist:
                                # handling characteristic cell_type
                                if c == "Cell_Type":
                                    cell_type = ''
                                    for p in self.specimen_cell_type:
                                        for q in self.cell_type:
                                            if p is None or q is None:
                                                continue
                                            elif s['RID'] == p['Specimen'] and p['Cell_Type'] == q['Name']:
                                                cell_type = cell_type + q['Name'] + ','
                                            else:
                                                continue
                                    characteristic = cell_type[:-1] if len(cell_type) > 0 else ''
                                    self.excel.write_cell(self.header_row_idx, local_col_idx,
                                                          'characteristics: Cell Type',
                                                          Style.HEADER)
                                    self.excel.write_cell(self.current_row_idx, local_col_idx, characteristic)
                                    local_col_idx += 1
                                # handling characteristic stage
                                elif c == "Stage":
                                    get_stage = 0
                                    for v in self.stage:
                                        if v is None:
                                            continue
                                        elif v['ID'] == s['Stage_ID']:
                                            characteristic = v['Name']
                                            self.excel.write_cell(self.header_row_idx, local_col_idx, 'characteristics: Stage', Style.HEADER)
                                            self.excel.write_cell(self.current_row_idx, local_col_idx, characteristic)
                                            local_col_idx += 1
                                            get_stage = 1
                                        else:
                                            continue
                                    # using Stage_Detail if no Stage_ID
                                    if get_stage == 0:
                                        characteristic = s.get('Stage_Detail', '')
                                        self.excel.write_cell(self.header_row_idx, local_col_idx, 'characteristics: Age', Style.HEADER)
                                        self.excel.write_cell(self.current_row_idx, local_col_idx, characteristic)
                                        local_col_idx += 1
                                
                                # Handling Specimen Allele Information
                                elif c == 'Allele':
                                    allele_info = []
                                    for sa in self.specimen_allele:
                                        if sa is None:
                                            continue
                                        elif s['RID'] == sa['Specimen_RID']:
                                                for a in self.allele:
                                                    if a is None:
                                                        continue
                                                    elif sa['Allele_RID'] == a['RID']:
                                                        if a.get('Allele_Type') is not None:
                                                            allele_info.append( a['Name'] + ' (' + a['Allele_Type'] + ')' )
                                                        else:
                                                            allele_info.append( a['Name'] )
                                    
                                    characteristic = ', '.join( allele_info ) if len(allele_info ) > 0 else ''
                                    self.excel.write_cell(self.header_row_idx, local_col_idx, 'characteristics: Allele', Style.HEADER )
                                    self.excel.write_cell(self.current_row_idx, local_col_idx, characteristic )
                                    local_col_idx += 1
                                    
                                else:
                                    characteristic = s.get(c, '')
                                    self.excel.write_cell(self.header_row_idx, local_col_idx, 'characteristics: '+c, Style.HEADER)
                                    self.excel.write_cell(self.current_row_idx, local_col_idx, characteristic)
                                    local_col_idx += 1

                            #sample description: from Specimen.Upload_notes
                            self.excel.write_cell(self.header_row_idx, local_col_idx, 'description', Style.HEADER)
                            self.excel.write_cell(self.current_row_idx, local_col_idx, s.get('Upload_Notes', ''))
                            local_col_idx += 1
                            
                        else:
                            continue


                    self.excel.write_cell(self.header_row_idx, local_col_idx, 'molecule', Style.HEADER)
                    self.excel.write_cell(self.current_row_idx, local_col_idx, sample_molecule)
                    local_col_idx += 1

                    # processed files in template are write to supplementary file
                    # 1. set flagWhitelist to False then all the files will pass
                    # 2. set flagWhitelist to True and fileEndingList to [] then no file will pass
                    # 3. set flagWhitelist to True and fileEndingList to [xxx,yyy] then only files ending with xxx,yyy will pass
                    flagWhitelist = self.replicate_file_filter
                    fileTypeWhiteList = self.replicate_file_ext_list
                    
                    for f in self.files:
                        validFile = False
                        if f is None or 'Replicate_RID' not in f.keys():
                            continue
                        elif flagWhitelist and fileTypeWhiteList:
                            for extension in fileTypeWhiteList:
                                if f.get('File_Name') is not None and f.get('File_Name').lower().endswith(extension):
                                    validFile = True
                                    break
                        else:
                            # if flagWhitelist == False then all the files are valid
                            validFile = True
                        
                        if validFile and f['Replicate_RID'] == r['RID']:
                            file_path = self.parameters.get("replicate_files_path_template", f.get("File_Name", ""))
                            self.excel.write_cell(self.header_row_idx, local_col_idx, 'raw file', Style.HEADER)
                            self.excel.write_cell(self.current_row_idx, local_col_idx, file_path.format(**f))
                            local_col_idx += 1
                        else:
                            continue
                else:
                    continue
        #print("export_sample: end")
    
    # export data for protocol section
    def export_protocol(self):
        #print("export_protocol: begin")
        self.current_row_idx = self.current_row_idx + 2
        self.excel.write_cell(self.current_row_idx, 1, 'PROTOCOLS', Style.SECTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              ('# Any of the protocols below which are applicable to only a subset of '
                               'Samples should be included as additional columns of the SAMPLES section instead.'),
                              Style.INSTRUCTION)
        # write some blank cells to make words of last cell showing expanded.
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1

        self.excel.write_cell(self.current_row_idx, 1, 'growth protocol', Style.FIELD)
        if self.protocol_unique['Growth_Protocol']:
            if len(self.protocol_list['Growth_Protocol']) == 1:
                self.excel.write_cell(self.current_row_idx, 2, self.protocol_list['Growth_Protocol'][0])
            else:
                self.excel.write_cell(self.current_row_idx, 2, '')
        else:
            self.excel.write_cell(self.current_row_idx, 2, '')
        self.current_row_idx += 1

        self.excel.write_cell(self.current_row_idx, 1, 'treatment protocol', Style.FIELD)
        if self.protocol_unique['Treatment_Protocol']:
            if len(self.protocol_list['Treatment_Protocol']) == 1:
                self.excel.write_cell(self.current_row_idx, 2, self.protocol_list['Treatment_Protocol'][0])
            else:
                self.excel.write_cell(self.current_row_idx, 2, '')
        else:
            self.excel.write_cell(self.current_row_idx, 2, '')
        self.current_row_idx += 1

        self.excel.write_cell(self.current_row_idx, 1, 'extract protocol', Style.FIELD)
        if self.protocol_unique['Extract_Protocol']:
            if len(self.protocol_list['Extract_Protocol']) == 1:
                self.excel.write_cell(self.current_row_idx, 2, self.protocol_list['Extract_Protocol'][0])
            else:
                self.excel.write_cell(self.current_row_idx, 2, '')
        else:
            self.excel.write_cell(self.current_row_idx, 2, '')
        self.current_row_idx += 1

        self.excel.write_cell(self.current_row_idx, 1, 'library construction protocol', Style.FIELD)
        if self.protocol_unique['Construction_Protocol']:
            if len(self.protocol_list['Construction_Protocol']) == 1:
                self.excel.write_cell(self.current_row_idx, 2, self.protocol_list['Construction_Protocol'][0])
            else:
                self.excel.write_cell(self.current_row_idx, 2, '')
        else:
            self.excel.write_cell(self.current_row_idx, 2, '')
        self.current_row_idx += 1

        for p in self.protocol_type:
            if p not in ['Growth_Protocol', 'Treatment_Protocol', 'Extract_Protocol', 'Construction_Protocol']:
                # only write protocol_types of unique content for all the samples in the same study
                # as protocl_types of non-unique content are already exported in sample section
                if self.protocol_unique[p] and len(self.protocol_list[p]) == 1:
                    self.excel.write_cell(self.current_row_idx, 1, (p.replace('_', ' ').lower()), Style.FIELD)
                    self.excel.write_cell(self.current_row_idx, 2, self.protocol_list[p][0])
                    self.current_row_idx += 1

        self.excel.write_cell(self.current_row_idx, 1, 'library strategy', Style.FIELD)

        library_type_list = []
        for es in self.experiment_settings:
            if es is None or 'Experiment_RID' not in es.keys():
                continue
            else:
                if es['Library_Type'] not in library_type_list:
                    library_type_list.append(es['Library_Type'])

        self.excel.write_cell(self.current_row_idx, 2, ','.join(library_type_list))
        #print("export_protocol: end")
    
    # export data processing pipline information
    def export_data_processing(self):
        #print("export_data_processing: begin")
        # DATA PROCESSING PIPELINE
        self.current_row_idx = self.current_row_idx + 2

        self.excel.write_cell(self.current_row_idx, 1, 'DATA PROCESSING PIPELINE', Style.SECTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              ('# Data processing steps include base-calling, alignment, filtering,'
                               'peak-calling, generation of normalized abundance measurements etc'), Style.INSTRUCTION)
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              ('# For each step provide a description, as well as software name,'
                               'version, parameters, if applicable.'), Style.INSTRUCTION)
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, '# Include additional steps, as necessary.', Style.INSTRUCTION)
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1


        if len(self.other_item_list['Data_Processing']) == 1:
            processing_list = self.other_item_list['Data_Processing'][0].split('\n')
        else:
            processing_list = []

        # include experiment setting parameters into data processing section
        other_procession_set = {}
        for es in self.experiment_settings:
            if es is None or 'Experiment_RID' not in es.keys():
                continue
            else:
                if other_procession_set:
                    for k, v in es.items():
                        if k in other_procession_set.keys() and v != other_procession_set[k]:
                            del other_procession_set[k]
                else:
                    other_procession_set = es.copy()

        for step_row in processing_list:
            self.excel.write_cell(self.current_row_idx, 1, 'data processing step', Style.FIELD)
            self.excel.write_cell(self.current_row_idx, 2, step_row)
            self.current_row_idx += 1

        for k, v in other_procession_set.items():
            #print('processing: %s %s' % (k, v))
            if k in ['RCB', 'RCT', 'RID', 'RMB', 'RMT']:
                continue
            
            # ignore static columns
            if k in ['Library','Visualization','Alignment','Sequencing_Method','Quantification']:
                continue

            # check truthiness except boolean and int type e.g. allow False and 0
            if v or isinstance(v, (bool, int, float)):
                self.excel.write_cell(self.current_row_idx, 1, 'data processing step', Style.FIELD)
                self.excel.write_cell(self.current_row_idx, 2, '%s : %s' % (k, v))
                self.current_row_idx += 1

        if self.other_item_unique['Reference_Genome']:
            if len(self.other_item_list['Reference_Genome']) == 1:
                self.excel.write_cell(self.current_row_idx, 1, 'genome build', Style.FIELD)
                self.excel.write_cell(self.current_row_idx, 2, self.other_item_list['Reference_Genome'][0])
            else:
                self.excel.write_cell(self.current_row_idx, 1, 'genome build', Style.FIELD)
                self.excel.write_cell(self.current_row_idx, 2, ' '.join(self.other_item_list['Reference_Genome']))
                logging.debug('Multi-genome')

        self.current_row_idx += 1

        self.excel.write_cell(self.current_row_idx, 1, 'processed data files format and content', Style.FIELD)
        self.excel.write_cell(self.current_row_idx, 2, 'processed data file format')
        self.current_row_idx += 1

        #print("export_data_processing: end")
    
    # export processed datafile names of this study
    def export_processed_datafiles(self):
        #print("export_processed_datafiles: begin")        
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              ('# For each file listed in the "processed data file" columns of '
                               'the SAMPLES section, provide additional information below.'), Style.INSTRUCTION)
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'PROCESSED DATA FILES', Style.SECTION)
        self.header_row_idx = self.current_row_idx + 1
        self.current_row_idx += 2


        # 1. set flagWhitelist to False then all the files will pass
        # 2. set flagWhitelist to True and fileEndingList to [] then no file will pass
        # 3. set flagWhitelist to True and fileEndingList to [xxx,yyy] then only files ending with xxx,yyy will pass
        flagWhitelist = self.study_file_filter
        fileTypeWhiteList = self.study_file_ext_list

        for pf in self.study_files:
            validFile = False
            if pf is None:
                continue
            elif flagWhitelist:
                for str in fileTypeWhiteList:
                    if pf.get('File_Name') is not None and pf.get('File_Name').lower().endswith(str):
                        validFile = True
                        break
            else:
                # if flagWhitelist == False then all the files are valid
                validFile = True
            if validFile:
                local_col_idx = 1
                file_path = self.parameters.get("study_files_path_template", pf.get("File_Name", ""))
                file_path = file_path.format(**pf)
                self.excel.write_cell(self.header_row_idx, local_col_idx, 'file name', Style.HEADER)
                self.excel.write_cell(self.current_row_idx, local_col_idx, file_path)
                local_col_idx += 1
                self.excel.write_cell(self.header_row_idx, local_col_idx, 'file type', Style.HEADER)
                self.excel.write_cell(self.current_row_idx, local_col_idx, pf.get('File_Type', ''))
                local_col_idx += 1
                self.excel.write_cell(self.header_row_idx, local_col_idx, 'file checksum', Style.HEADER)
                self.excel.write_cell(self.current_row_idx, local_col_idx, pf.get('MD5', ''))
                self.current_row_idx += 1
        #print("export_processed_datafiles: end")        

    # export raw datafile names of this study
    def export_raw_datafiles(self):
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, ('# For each file listed in the "raw file" columns of'
                                                        'the SAMPLES section, provide additional information below.'),
                              Style.INSTRUCTION)
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'RAW FILES', Style.SECTION)
        self.header_row_idx = self.current_row_idx + 1
        self.current_row_idx += 2


        # Group the File Names by Replicate RID's
        replicateFileDict = {}    
        for pf in self.files:
            if pf is None or pf.get('Replicate_RID') is None or pf.get('File_Name') is None:
                continue
            elif pf['Replicate_RID'] in replicateFileDict:
                replicateFileDict[ pf['Replicate_RID'] ].append( pf['File_Name'] )
            else:
                replicateFileDict[ pf['Replicate_RID'] ] = [ pf['File_Name'] ]


        derivedPairedEnds = {}
        # fileTypeWhiteList = {} # https://github.com/informatics-isi-edu/rbk-project/issues/652
        patternR1 = ".*(\.|_)R1(\.|_)[0-9]*(\.|_)*(FASTQ|FQ).GZ"
        patternR2 = ".*(\.|_)R2(\.|_)[0-9]*(\.|_)*(FASTQ|FQ).GZ"
        for replicate_rid, fileList in replicateFileDict.items():
            ## Derive the paired end values using replicate level files.
            ## https://github.com/informatics-isi-edu/rbk-project/issues/615
            if ( any( re.match( patternR1, fileName.upper() ) for fileName in fileList ) 
                and any( re.match( patternR2, fileName.upper() ) for fileName in fileList ) ):
                derivedPairedEnds[ replicate_rid ] = 'paired-end'
            elif any( re.match( patternR1, fileName.upper() ) for fileName in fileList ):
                derivedPairedEnds[ replicate_rid ] = 'single'
            else:
                derivedPairedEnds[ replicate_rid ] = None

            ## GEO prefer bams/sams, but will accept fastq’s if that is all that is present. 
            ## Add logic to set priority of seq files to export
            ## https://github.com/informatics-isi-edu/rbk-project/issues/652
            ##
            ## THIS CHANGE IS PUT ON HOLD!! 
            ## until we can find a way to filter the files in the export BDBags as well to keep the data consistent.
            ##############################################################################################################
            
            # if ( any( re.match( '.*\.(bam|sam|bai)', fileName.lower() ) for fileName in fileList ) ):
            #     fileTypeWhiteList[ replicate_rid ] = [ ".bam" ]
            # else:
            #     fileTypeWhiteList[ replicate_rid ] = [ ".fastq", ".fastq.gz" ]


        # 1. set flagWhitelist to False then all the files will pass
        # 2. set flagWhitelist to True and fileEndingList to [] then no file will pass
        # 3. set flagWhitelist to True and fileEndingList to [xxx,yyy] then only files ending with xxx,yyy will pass
        flagWhitelist = self.replicate_file_filter
        fileTypeWhiteList = self.replicate_file_ext_list
        for pf in self.files:
            validFile = False
            if pf is None or 'Experiment_RID' not in pf.keys():
                continue
            elif flagWhitelist and fileTypeWhiteList:
                # for str in fileTypeWhiteList[ pf.get('Replicate_RID' ) ]: # https://github.com/informatics-isi-edu/rbk-project/issues/652
                for str in fileTypeWhiteList:
                    if pf.get('File_Name') is not None and pf.get('File_Name').lower().endswith(str):
                        validFile = True
                        break
            else:
                # if flagWhitelist == False then all the files are valid
                validFile = True
            #print("%s %s" %(pf.get("File_Name", ""), validFile))
            if validFile:
                current_col_idx = 1
                file_path = self.parameters.get("replicate_files_path_template", pf.get("File_Name", ""))
                self.excel.write_cell(self.header_row_idx, current_col_idx, 'file name', Style.HEADER)
                self.excel.write_cell(self.current_row_idx, current_col_idx, file_path.format(**pf))
                current_col_idx += 1
                self.excel.write_cell(self.header_row_idx, current_col_idx, 'file type', Style.HEADER)
                self.excel.write_cell(self.current_row_idx, current_col_idx, pf.get('File_Type', ''))
                current_col_idx += 1
                self.excel.write_cell(self.header_row_idx, current_col_idx, 'file checksum', Style.HEADER)
                self.excel.write_cell(self.current_row_idx, current_col_idx, pf.get('MD5', ''))
                current_col_idx += 1
                # Experiment_Settings.Protocol_Reference
                for es in self.experiment_settings:
                    if es is None or 'Experiment_RID' not in es.keys():
                        continue
                    elif es['Experiment_RID'] == pf['Experiment_RID']:
                        self.excel.write_cell(self.header_row_idx, current_col_idx, 'instrument model', Style.HEADER)
                        self.excel.write_cell(self.current_row_idx, current_col_idx, es.get('Sequencing_Platform', ''))
                        current_col_idx += 1
                        
                        
                        ## Remove Read_length from the excel ( https://github.com/informatics-isi-edu/rbk-project/issues/667 )
                        ## If we do add Read Length GEO team will either ignore it (raw file section) or print it if you put it in the data processing.
                        # self.excel.write_cell(self.header_row_idx, current_col_idx, 'read length', Style.HEADER)
                        # self.excel.write_cell(self.current_row_idx, current_col_idx, es.get('Read Length', ''))
                        # current_col_idx += 1
                        
                        ## Modify the paired end logic to fetch derived values.
                        ## https://github.com/informatics-isi-edu/rbk-project/issues/615
                        ## OLD CODE
                        # if es.get('Paired_End') is not None and 'pair' in es.get('Paired_End').lower():
                        #     single_or_paired = 'paired-end'
                        # else:
                        #     single_or_paired = 'single'
                        
                        ## NEW CODE
                        # If the paired end value could not be derived then switch to the value in Experiment Settings
                        if ( derivedPairedEnds is None
                            or pf.get('Replicate_RID') is None
                            or pf['Replicate_RID'] not in derivedPairedEnds
                            or derivedPairedEnds [ pf['Replicate_RID'] ] is None ):
                            ## OLD CODE
                            if es.get('Paired_End') is not None and 'pair' in es.get('Paired_End').lower():
                                single_or_paired = 'paired-end'
                            else:
                                single_or_paired = 'single'
                        else:
                            single_or_paired = derivedPairedEnds[ pf['Replicate_RID'] ]                        
                        ## MODIFICATION END

                        self.excel.write_cell(self.header_row_idx, current_col_idx, 'single or paired-end',
                                              Style.HEADER)
                        self.excel.write_cell(self.current_row_idx, current_col_idx, single_or_paired)
                        current_col_idx += 1

                self.current_row_idx += 1
    
    def export_paired_end(self):
        #print("export_paired_end: begin")        
        # PAIRED-END EXPERIMENTS
        # todo need check what's paired-end data look like
        # currently, we haven't seen paired-end data
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, (
            '# For paired-end experiments, list the 2 associated raw files, and provide average insert size and standard deviation,'
            'if known. For SOLiD experiments, list the 4 file names (include "file name 3" and "file name 4" columns).'),
                              Style.INSTRUCTION)
        for col in range(2, 13):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'PAIRED-END EXPERIMENTS', Style.SECTION)
        self.header_row_idx = self.current_row_idx + 1
        self.current_row_idx += 1
        current_col_idx = 1
        self.excel.write_cell(self.header_row_idx, current_col_idx, 'file name 1', Style.HEADER)
        current_col_idx += 1
        self.excel.write_cell(self.header_row_idx, current_col_idx, 'file name 2', Style.HEADER)
        current_col_idx += 1
        self.excel.write_cell(self.header_row_idx, current_col_idx, 'average insert size', Style.HEADER)
        current_col_idx += 1
        self.excel.write_cell(self.header_row_idx, current_col_idx, 'standard deviation', Style.HEADER)
        #print("export_paired_end: end")

    
    def export_finish(self):
        self.excel.save_xlsx()
