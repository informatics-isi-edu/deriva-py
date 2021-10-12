import sys
import os
import io
import json
import importlib
import logging
import re
from copy import copy
from enum import Enum
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.transform.base_transform_processor import BaseTransformProcessor, \
    LOCAL_PATH_KEY, SOURCE_URL_KEY
from collections import OrderedDict

logger = logging.getLogger(__name__)


def conflate_falsish(v, falsish=''):
    if isinstance(v, (bool, int, float)) or v:
        return v
    else:
        return falsish

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
        geo = Export2GEO(self.input_abspath, self.output_abspath, self.parameters, self.envars.get("hostname"))
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

        self.ws.column_dimensions['A'].width = 45.0
        self.ws.column_dimensions['B'].width = 40.0
        self.ws.column_dimensions['C'].width = 24.0
        self.ws.column_dimensions['D'].width = 15.0
        for col_index in [ chr(x) for x in range( ord('A'), ord('Z') ) ]:
            self.ws.column_dimensions[col_index].width = 24.0

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

    def __init__(self, input_file, output_excel_path, parameters, hostname):
        self.__name__ = 'export_geo_template'
        self.input_file = input_file
        self.output_excel_path = output_excel_path
        self.parameters = parameters
        self.hostname = hostname
        self.server_env = hostname.split(".")[0]
        #logging.debug("%s %s %s %s" % (input_file, output_excel_path, parameters, hostname))
        
        self.MAX_SECTION_COLUMN_IDX = 15
        self.current_row_idx = 0
        self.header_row_idx = 0

        self.protocol_global_content_dict = {}        
        self.protocol_local = []

        self.data_processing_global_dict = {}
        self.reference_genome_set = set()
        self.data_processing_local = []
        self.INCLUDE_DATA_PROCESSING_LOCAL = False
        
        # A mapping between different protocol type to protocol columns in the Experiment table
        # There are 2 columns related to each protocol, therefore the tuple.
        self.protocol_dict = OrderedDict([
            ('growth protocol', ('Growth_Protocol', 'Growth_Protocol_Reference')),
            ('treatment protocol', ('Treatment_Protocol', 'Treatment_Protocol_Reference')),
            ('extract protocol', ('Extract_Protocol', 'Extract_Protocol_Reference')),
            ('library construction protocol', ('Construction_Protocol', 'Construction_Protocol_Reference')),
            ('isolation protocol', ('Isolation_Protocol', 'Isolation_Protocol_Reference')),
            ('label protocol', ('Label_Protocol', 'Label_Protocol_Reference')),
            ('hybridization protocol', ('Hybridization_Protocol', 'Hybridization_Protocol_Reference')),
            ('scan protocol', ('Scan_Protocol', 'Scan_Protocol_Reference')),
            ('library type', ('Library_Type', 'Library_Type_Reference')),            
        ])
        self.protocol_mandatory_list = ['growth protocol', 'treatment protocol', 'extract protocol', 'library construction protocol']

        # A list of columns in Experiment_Settings related to "Data Processing Section"
        # remove alignment since we won't export bam file. GEO prefers bam file than fastq files. If export both, they ask users to remove fastq.
        # no need for analysis metadata 
        self.data_processing_list = [
            'Data_Processing',                                     # This property is derived from Experiment
            #'Library_Type', 'Protocol_Reference',                 # library---remove since they are included under Protocol instead
            'Library_Selection', 'Strandedness', 'Used_Spike_Ins', 'Spike_Ins_Amount',   # library
            'Sequencing_Platform',  'Paired_End', 'Read_Length',   # sequencing
            #'Alignment_Format', 'Aligner', 'Aligner_Version',     # alignment---remove since we don't provide bam for now
            'Reference_Genome',
            'Quantification_Format', 'Expression_Metric', 'Quantification_Software', 'Transcriptome_Model',     #quantification
            'Visualization_Format', 'Visualization_Software', 'Visualization_Version', 'Visualization_Setting', # visuallization           
            'Duplicate_Removal', 'Sequence_Trimming', 'Pre-alignment_Sequence_Removal', 'Junction_Reads',     # analysis
            #'Typeof_Reads',  # deprecated
            'Notes'
        ]

        # direct Specimen properties that will show up in sample column headers
        # the keys will be in order when iterate        
        self.characteristics_dict = OrderedDict([
            ('Species', 'organism'),            
            ('Tissue_Names', 'source name'),
            ('Cell_Type_Names', 'characteristics: Cell Type'),
            ('Allele_Names', 'characteristics: Allele'),                        
            ('Assay_Type', 'characteristics: Assay Type'),
            ('Sex', 'characteristics: Sex'),
            ('Stage_Name', 'characteristics: Stage'),
            ('Stage_Detail', 'characteristics: Age'),
            ('Phenotype', 'characteristics: Phenotype'),
            ('Strain','characteristics: Strain'),
            ('Wild_Type', 'characteristics: Wild Type'),
            ('Passage', 'characteristics: Passage'),
            ('Upload_Notes', 'characteristics: Notes'), # change from Description to Notes
        ])

        # whether to filter study_file
        # note: ext should all be lower case as we lower case the filename before comparison
        self.study_file_pattern = ".*([.](csv|tsv|txt|mtx|bw|xlsx|h5)([.]gz)?)$"        
        # NOTE: GEO only wants fastq or bam. They prefer bam. But we do not have a way to write conditional fetch e.g. if bam, only download bam
        # For now, remove bam from the white list
        self.replicate_raw_file_pattern = ".*(r[12][.]fastq([.]gz)?)$"
        self.replicate_processed_file_pattern = ".*([.](csv|tsv|txt|mtx|bw|xlsx|h5)([.]gz)?)$"
        
        # new structure to be created
        self.experiment_dict = {}
        self.experiment_setting_dict = {}
        self.replicate_dict = {}
        self.file_dict = {}

        self.study_processed_files = []        
        self.replicate_raw_file_dict = {}
        self.replicate_processed_file_dict = {}
        self.INCLUDE_STUDY_FILES_IN_PROCESSED_FILES_SECTION = False
        
        self.initialize_data()

    def organize_experiments(self):
        # add library_strategy from experiment_setting to experiment
        for ex in self.experiment_setting_dict.values():
            self.experiment_dict[ex["Experiment_RID"]]["Library_Type"] = ex["Library_Type"]
            self.experiment_dict[ex["Experiment_RID"]]["Library_Type_Reference"] = ""

    # gathering missing fields in experiment_settings for Data Processing section
    def organize_experiment_settings(self):
        for ex in self.experiment_dict.values():
            self.experiment_setting_dict[ex["RID"]]["Data_Processing"] = conflate_falsish(ex.get("Data_Processing"), "")
        
    def organize_specimens(self):
        data = self.data
        stage = data[0].get('Stage')
        tissue = data[0].get('Tissue')
        source_type = data[0].get('Source')
        specimen_cell_type = data[0].get('Specimen_Cell_Type')
        cell_type = data[0].get('Cell_Type')
        specimen_allele = data[0].get('Specimen_Allele')
        allele = data[0].get('Allele')
        
        # -- create a list of tissue_name_set per specimen
        tissue_name_dict = {
            s["ID"]: s["Name"]  # key = value
            for s in (source_type if source_type else []) #iteration
            if s is not None  # condition
        }
        # tissue_list_dict with Specimen_RID as key
        specimen_tissue_name_set_dict = {}
        for r in (tissue if tissue else []): #iteration
            if r is None:
                continue
            specimen_tissue_name_set_dict.setdefault(r["Specimen_RID"], set()).add(tissue_name_dict[r["Tissue"]])
        # -- end

        # create a list of cell_type_name_set per specimen
        # Note: no need for cell_type_dict.
        specimen_cell_type_name_set_dict = {}
        for r in (specimen_cell_type if specimen_cell_type else []):
            if r is None:
                continue
            specimen_cell_type_name_set_dict.setdefault(r["Specimen"], set()).add(r["Cell_Type"])
            #logging.debug("-- Specimen cell type: %s %s" % (r["Specimen"], r["Cell_Type"]))
        
        # -- create a list of allele_name_set per specimen
        allele_name_dict = {
            r["RID"]: r["Name"] + ((' (' + r['Allele_Type'] + ')') if r['Allele_Type'] else '')
            for r in (allele if allele else []) #iteration
            if r is not None  # condition
        }
        # specimen_allele_name_set_dict with Specimen_RID as key
        specimen_allele_name_set_dict = {}
        for r in (specimen_allele if specimen_allele else []):
            if r is None:
                continue
            specimen_allele_name_set_dict.setdefault(r["Specimen_RID"], set()).add(allele_name_dict[r["Allele_RID"]])
        # -- end

        # stage lookup
        stage_dict = {
            r["ID"]: r
            for r in (stage if stage else []) #iteration
            if r is not None  # condition
        }

        # -- update Specimen
        for s in self.specimen_dict.values():
            specimen_rid = s["RID"]
            s["Stage_Name"] = stage_dict[s["Stage_ID"]]["Name"] if s["Stage_ID"] in stage_dict else ''
            s["Tissue_Names"] = ''.join(specimen_tissue_name_set_dict[specimen_rid]) if specimen_rid in specimen_tissue_name_set_dict else ''
            s["Cell_Type_Names"] = ''.join(specimen_cell_type_name_set_dict[specimen_rid]) if specimen_rid in specimen_cell_type_name_set_dict else ''                        
            s["Allele_Names"] = ''.join(specimen_allele_name_set_dict[specimen_rid]) if specimen_rid in specimen_allele_name_set_dict else ''
        
    ''' Organize replicate raw files and processed files
    '''
    def organize_replicate_files(self):
        # Create a file_dics lookup based on Relicate_RID
        # Assume File_Name is not null. TODO: what to do if File_Name is not null
        replicate_file_dict = {}
        for f in self.file_dict.values():
            if f is None:
                continue
            # setdefault idiom. Run help(dict.setdefault) for explanation
            replicate_file_dict.setdefault(f["Replicate_RID"], set()).add(f["RID"])
        
        # determine where files should go and update the global raw and processed file list
        for replicate_rid, files in replicate_file_dict.items():
            raw_files = []
            processed_files = []
            has_r1 = False
            has_r2 = False

            #logging.debug("begin: %s %s" % (replicate_rid, files))
            # put file into raw v.s. processed v.s. ignored
            for file_rid in files:
                file = self.file_dict[file_rid]
                filename = file["File_Name"]
                # check raw_file
                if re.match(self.replicate_raw_file_pattern, filename, re.IGNORECASE):
                    raw_files.append(file)
                elif re.match(self.replicate_processed_file_pattern, filename, re.IGNORECASE):
                    processed_files.append(file)
                else:
                    # ignore
                    pass
                    
                if re.match("r1[.]fastq([.]gz)?$", filename, re.IGNORECASE):
                    has_r1 = True
                elif re.match("r2[.]fastq([.]gz)?$", filename, re.IGNORECASE):
                    has_r2 = True
                    
            # determine paired_end based on files
            if has_r1 and has_r2:
                paired_end = 'paired-end'
            elif has_r1:
                paired_end = 'single'
            else:
                paired_end = None                
                # set the metadata from experiment_settings only if raw_files exist
                if not raw_files:
                    pass
                #logging.debug("%r" % (f,))
                experiment_rid = self.replicate_dict[replicate_rid]["Experiment_RID"]
                if  experiment_rid in self.experiment_setting_dict:
                    if self.experiment_setting_dict[experiment_rid]["Paired_End"] == "Paired End":
                        paired_end = 'paired-end'
                    elif self.experiment_setting_dict[experiment_rid]["Paired_End"] == "Single End":
                        paired_end = 'single'                        

            # update Paired_End of raw files
            for f in raw_files:
                f["Paired_End"] = paired_end
                #logging.debug("-- raw %s : %s %s" % (replicate_rid, f["File_Name"], paired_end))

            for f in processed_files:            
                logging.debug("-- processed %s : %s " % (replicate_rid, f["File_Name"]))
                
            # update global raw and processed file
            if raw_files:
                self.replicate_raw_file_dict[replicate_rid] = raw_files
            if processed_files:
                self.replicate_processed_file_dict[replicate_rid] = processed_files

        if False:
            for r, files in self.replicate_raw_file_dict.items():
                for f in files:
                    logging.debug("replicate_raw_file_dict: %s %s" % (f["File_Name"], f["Paired_End"]))
            for r, files in self.replicate_processed_file_dict.items():
                for f in files:
                    logging.debug("replicate_processed_file_dict: %s " % (f["File_Name"]))
        
    # TODO: extend DerivaDownloadError class to extend error class for custom error messages. This error cause a 502 error from the export service
    # You can set a proper message into DerivaDownloadError or your custom error, but should check if chaise displays that message when it gets the 502
    def initialize_data(self):
        logging.debug("initilaize_data: begin")
        with io.open(self.input_file, encoding='utf-8') as input_file:
            data = json.load(input_file)

        self.study = None
        # TODO: Create a proper error message            
        if not data:
            logging.debug("No Data")
            return
        
        # TODO: create a proper error message            
        if len(data) > 1:
            logging.debug("Duplicate Data")
            return

        # when there is only 1 study
        self.data = data
        # get parameters from json file
        self.study = data[0].get('Study')[0]
        self.study_files = data[0].get('Study_File')        
        self.pi = data[0].get('Principal_Investigator')[0]

        experiments = data[0].get('Experiment')
        experiment_settings = data[0].get('Experiment_Setting')
        replicates = data[0].get('Replicate')
        specimens = data[0].get('Specimen')
        files = data[0].get('File')
        
        # create a specimen_dict. Only include specimen that are sequencing type
        self.specimen_dict = {
            s["RID"]: s  # key = value
            for s in (specimens if specimens else []) #iteration
            if s is not None and re.match("(.*-Seq|NextGen)$", s["Assay_Type"], re.IGNORECASE) # condition
        }
        
        # create a replicate_dict. Using succint syntax.
        # Include only sequencing replicates
        self.replicate_dict = {
            r["RID"]: r  # key = value
            for r in (replicates if replicates else []) #iteration
            if r is not None and (r["Specimen_RID"] in self.specimen_dict)  # condition
        }

        exp_with_seq_replicates = {r["Experiment_RID"] for r in self.replicate_dict.values() }
        
        # -- create a dictionary for looking up different data
        # create a experiment_dict that is of Seq type only
        self.experiment_dict = {
            e["RID"]: e  # key = value
            for e in (experiments if experiments else []) #iteration
            if e is not None and e["RID"] in exp_with_seq_replicates # condition
        }
        
        #
        # TODO: if there is no seq experiment, throw an error
        #
        
        # create a experiment_settings_dict. Using succint syntax
        self.experiment_setting_dict = {
            e["Experiment_RID"]: e  # key = value
            for e in (experiment_settings if experiment_settings else []) #iteration
            if e is not None and (e["Experiment_RID"] in self.experiment_dict) # condition
        }

        # create a file_dict. Using succint syntax
        self.file_dict = {
            f["RID"]: f  # key = value
            for f in (files if files else []) #iteration
            if f is not None and (f["Replicate_RID"] in self.replicate_dict)  # condition
        }

        # -- update self.study_files, self.replicate_raw_files, self.replicate_processed_files
        # create self.study_files
        for f in (self.study_files if self.study_files else []):
            if f is None:
                continue
            if re.match(self.study_file_pattern, f["File_Name"], re.IGNORECASE):
                self.study_processed_files.append(f)
        
        # sort the raw and processed files
        self.study_processed_files.sort( key = lambda x: (x.get('File_Name')))
        # -- debug
        #for f in self.study_processed_files:
        #    logging.debug("study: %s " % (f["File_Name"]))                

        # -- organizing data from different tables in the appropriate structure so data in different sections can be retrieved uniformly
        self.organize_experiments()
        self.organize_experiment_settings()                
        self.organize_specimens()
        self.organize_replicate_files()

        #
        # TODO: Add validation to check that all replicates have at least 1 raw files
        #
        
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

        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
            for row in range(1, 7):
                self.excel.write_cell(row, col, '', Style.PREFACE)
        #print("export_preface: end")

    # export SERIES(study)
    def export_series(self):
        ##print("export_series: begin")
        s = self.study
        self.excel.write_cell(self.current_row_idx, 1, 'SERIES', Style.SECTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, '# This section describes the overall experiment.',
                              Style.INSTRUCTION)
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
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

        consortium_study_id = ''
        if s["Consortium"] == 'GUDMAP':
            consortium = 'GUDMAP Consortium'            
            consortium_study_id = ''.join(['https://', self.server_env, '.', 'gudmap.org/id/', s["RID"]])
        elif s["Consortium"] == 'RBK':
            consortium = '(Re)Building A Kidney (RBK) Consortium'                        
            consortium_study_id = ''.join(['https://', self.server_env, '.', 'rebuildingakidney.org/id/', s["RID"]])
        if consortium_study_id:
            study_source = "Source: " + consortium + ", Study Record ID: " + consortium_study_id
            # remove blank space
            #self.excel.write_cell(self.current_row_idx, 1, 'overall design', Style.FIELD)
            #self.excel.write_cell(self.current_row_idx, 2, '')
            #self.current_row_idx += 1
            self.excel.write_cell(self.current_row_idx, 1, 'overall design', Style.FIELD)
            self.excel.write_cell(self.current_row_idx, 2, study_source)
            self.current_row_idx += 1
        
        self.excel.write_cell(self.current_row_idx, 1, 'contributor', Style.FIELD)
        self.excel.write_cell(self.current_row_idx, 2, self.pi.get('Full_Name', ''))
        self.current_row_idx += 1

        # study-level processed file: one line for each study file
        for pf in self.study_processed_files:
            file_path = self.parameters.get("study_files_path_template", pf.get("File_Name", ""))
            self.excel.write_cell(self.current_row_idx, 1, 'supplementary file', Style.FIELD)
            self.excel.write_cell(self.current_row_idx, 2, file_path.format(**pf))
            self.current_row_idx += 1

        # write an empty line (to be over-written if the self.study_processed_file is empty        
        if not self.study_processed_files:
            self.excel.write_cell(self.current_row_idx, 1, 'supplementary file', Style.FIELD)        
            self.excel.write_cell(self.current_row_idx, 2, '')
            self.current_row_idx += 1
            
        #print("export_series: end")
        
    # prepare the data for sample export
    def export_sample_prepare(self):
        #print("export_sample_prepare: begin")

        # -- protocol
        # Check shared protocols: protocols that are the same for all experiments with seq replicates
        # Protocols which are applicable to only a subset of Samples should be included
        # as additional columns of the SAMPLES section instead.
        
        # protocol_global_dict stores items that are globally unique
        self.protocol_local = []
        self.protocol_global_dict = {}
        
        for p, col_list in self.protocol_dict.items():
            protocol_set = set()
            for e in self.experiment_dict.values():
                #print("%s: %s, %s : %s, %s" % (e["RID"], col_list[0], col_list[1], e.get(col_list[0]), e.get(col_list[1])))
                protocol_tuple = tuple( conflate_falsish(e.get(col), '') for col in col_list )
                protocol_set.add(protocol_tuple)
                if len(protocol_set) > 1:
                    break
            protocol_len = len(protocol_set)
            if protocol_len == 1:
                protocol_tuple = protocol_set.pop()
                if ''.join(protocol_tuple):
                    self.protocol_global_dict[p] = protocol_tuple
            elif protocol_len > 1:                 # there is content                
                self.protocol_local.append(p)
                
        logging.debug("-- global protocol : %s" % (self.protocol_global_dict.keys()))
        logging.debug("++ local protocol: %s" % (self.protocol_local))

        # -- data processing steps
        # Check shared data processing: experiment settings that are the same for all experiments with seq replicates
        # Properties which are applicable to only a subset of Samples should be included
        # as additional columns of the SAMPLES section instead.

        self.data_processing_local = []
        self.data_processing_global_dict = {}
        
        for dp in self.data_processing_list:
            data_processing_set = set()
            for es in self.experiment_setting_dict.values():
                content = str(conflate_falsish(es.get(dp), ''))
                data_processing_set.add(content)
                if len(data_processing_set) > 1:
                    #logging.debug("!!dp:%s --> Experiment_RID:%s content:%s set:%s " % (dp, es["Experiment_RID"], content, data_processing_set))
                    break
            data_processing_len = len(data_processing_set)
            if data_processing_len == 1:
                if content:
                    self.data_processing_global_dict[dp] = content
            elif data_processing_len > 1:
                self.data_processing_local.append(dp)
                
        logging.debug("-- global data_processing : %s" % (self.data_processing_global_dict.keys()))
        logging.debug("++ local data_processing: %s" % (self.data_processing_local))

        # -- reference genome under Data Processing section
        # Build Reference Genome Set for including in the Data Processing section
        self.reference_genome_set = set()        
        for e in self.experiment_setting_dict.values():
            content = conflate_falsish(e.get("Reference_Genome"), '')
            if content:            
                self.reference_genome_set.add(content)
                

    def format_protocol_tuple(self, protocol_tuple):
        content =  protocol_tuple[0] + (("\nReference: " + protocol_tuple[1]) if protocol_tuple[1] else '')        
        #logging.debug("format_protocol_tuple: %s --> %s" % (protocol_tuple, content))
        return content

    
    def print_experiment_protocol(self, experiment, protocol):
        cols = self.protocol_dict[protocol]
        col1 = conflate_falsish(experiment.get(cols[0]), '')
        col2 = conflate_falsish(experiment.get(cols[1]), '')
        content =  col1 + (("\nReference: " + col2) if col2 else '')
        return content
        
        
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
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              '# Additional "processed data file" or "raw file" columns may be included.',
                              Style.INSTRUCTION)
        # write some blank cells to make words of last cell showing expanded.
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.header_row_idx = self.current_row_idx + 1
        self.current_row_idx += 1

        # check the existance of specimen direct properties for all specimens. These will form the column headings
        null_characteristics = set(self.characteristics_dict.keys())
        for s in self.specimen_dict.values():
            if not null_characteristics:
                break
            for c in list(null_characteristics):
                if s.get(c):
                    null_characteristics.remove(c) 
        
        characteristics_exist = []
        for c in self.characteristics_dict.keys():
            if c not in null_characteristics:            
                characteristics_exist.append(c) 

        # == write SAMPLES
        # experiment by experiment
        for r in self.replicate_dict.values():
            specimen_rid = r["Specimen_RID"]
            if specimen_rid in self.specimen_dict:
                specimen = self.specimen_dict[specimen_rid]
            else:
                continue
            
            if r['Experiment_RID'] in self.experiment_dict:
                e = self.experiment_dict[r['Experiment_RID']]
                bio_replicate = str(r.get("Biological_Replicate_Number"))
                tech_replicate = str(r.get("Technical_Replicate_Number"))
                # construct sample_name using internal_ID,Biological_Replicate_Number and Technical_Replicate_Number
                sample_name = ''.join([ e.get('Internal_ID', ''), '_', bio_replicate, '_', tech_replicate])
                sample_title = ''.join([
                    e.get('Name', ''),
                    ' (biological replicate ', bio_replicate,
                    ', technical replicate ', tech_replicate,
                    ')'
                ])
                sample_molecule = e.get('Molecule_Type', '')

                
            # maximum number of replicate raw files
            max_num_raw_files = 0
            for v in self.replicate_raw_file_dict.values():
                num_files = len(v)
                if num_files > max_num_raw_files:
                    max_num_raw_files = num_files 

            # -- start writing
            local_col_idx = 1
            self.current_row_idx += 1

            # -- properties from experiment
            self.excel.write_cell(self.header_row_idx, local_col_idx, 'sample name', Style.HEADER)
            self.excel.write_cell(self.current_row_idx, local_col_idx, sample_name)
            local_col_idx += 1
            self.excel.write_cell(self.header_row_idx, local_col_idx, 'title', Style.HEADER)
            self.excel.write_cell(self.current_row_idx, local_col_idx, sample_title)
            local_col_idx += 1
            self.excel.write_cell(self.header_row_idx, local_col_idx, 'molecule', Style.HEADER)
            self.excel.write_cell(self.current_row_idx, local_col_idx, sample_molecule)
            local_col_idx += 1
            
            # -- Specimen properties
            if r["Specimen_RID"] in self.specimen_dict:
                s = self.specimen_dict[r["Specimen_RID"]]
                for c in characteristics_exist:
                    characteristic = s.get(c, '')
                    self.excel.write_cell(self.header_row_idx, local_col_idx, self.characteristics_dict[c], Style.HEADER)
                    self.excel.write_cell(self.current_row_idx, local_col_idx, characteristic)
                    local_col_idx += 1

            # -- Reference to replicate ID
            column_name = ''.join(["characteristics: ", r["Consortium"], " Record ID"])
            consortium_id = ''
            if r["Consortium"] == 'GUDMAP':
                consortium_id = ''.join(['https://', self.server_env, '.', 'gudmap.org/id/', r["RID"]])
            elif r["Consortium"] == 'RBK':
                consortium_id = ''.join(['https://', self.server_env, '.', 'rebuildingakidney.org/id/', r["RID"]])
            if consortium_id:
                self.excel.write_cell(self.header_row_idx, local_col_idx, column_name, Style.HEADER)
                self.excel.write_cell(self.current_row_idx, local_col_idx, consortium_id)
                local_col_idx += 1

            # -- add local data processing here under characteristics
            if self.INCLUDE_DATA_PROCESSING_LOCAL and (r['Experiment_RID'] in self.experiment_setting_dict):
                es = self.experiment_setting_dict[r['Experiment_RID']]
                for dp in self.data_processing_local:
                    column_name = "characteristics: " + dp
                    content = str(conflate_falsish(es.get(dp), ''))
                    self.excel.write_cell(self.header_row_idx, local_col_idx, column_name, Style.HEADER)
                    self.excel.write_cell(self.current_row_idx, local_col_idx, content)
                    local_col_idx += 1
            
            # -- local protocol
            # if different contents for protocol_type, need to add column for this protocol_type
            # then different samples may has different content to write
            for p in self.protocol_local:
                col_list = self.protocol_dict[p]
                protocol_tuple = tuple( conflate_falsish(e.get(col), '') for col in col_list )
                content = self.format_protocol_tuple(protocol_tuple)
                self.excel.write_cell(self.header_row_idx, local_col_idx, p, Style.HEADER)
                self.excel.write_cell(self.current_row_idx, local_col_idx, content)
                local_col_idx += 1

            # checkpoint before file listing, so begging of processed_data_file column can be determined
            processed_file_col_idx = local_col_idx + max_num_raw_files

            # -- raw files
            if r["RID"] in self.replicate_raw_file_dict:
                for f in self.replicate_raw_file_dict[r["RID"]]:
                    file_path = self.parameters.get("replicate_files_path_template", f.get("File_Name", ""))
                    self.excel.write_cell(self.header_row_idx, local_col_idx, 'raw file', Style.HEADER)
                    self.excel.write_cell(self.current_row_idx, local_col_idx, file_path.format(**f))
                    local_col_idx += 1

            # -- processed files
            # replicate processed files. Move col to end of raw files
            local_col_idx = processed_file_col_idx
            if r["RID"] in self.replicate_processed_file_dict:
                for f in self.replicate_processed_file_dict[r["RID"]]:
                    file_path = self.parameters.get("replicate_files_path_template", f.get("File_Name", ""))
                    self.excel.write_cell(self.header_row_idx, local_col_idx, 'processed data file', Style.HEADER)
                    self.excel.write_cell(self.current_row_idx, local_col_idx, file_path.format(**f))
                    local_col_idx += 1
            else:
                self.excel.write_cell(self.header_row_idx, local_col_idx, 'processed data file', Style.HEADER)
                self.excel.write_cell(self.current_row_idx, local_col_idx, 'none')
                local_col_idx += 1
        
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
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1

        # print the mandatory protocol first, regardless of content
        for p in self.protocol_mandatory_list:
            if p in self.protocol_global_dict:
                content = self.format_protocol_tuple(self.protocol_global_dict[p])
            else:
                content = ''
            #logging.debug(" -- global protocol (required) %s : %s --" % (p, content))
            self.excel.write_cell(self.current_row_idx, 1, p, Style.FIELD)
            self.excel.write_cell(self.current_row_idx, 2, content)
            self.current_row_idx += 1

        # print the rest only if there is content
        for p in self.protocol_global_dict:
            if p in self.protocol_mandatory_list:
                continue
            content = self.format_protocol_tuple(self.protocol_global_dict[p])
            #logging.debug(" -- global protocol (optional) %s : %s --" % (p, content))            
            self.excel.write_cell(self.current_row_idx, 1, p, Style.FIELD)
            self.excel.write_cell(self.current_row_idx, 2, content)            
            self.current_row_idx += 1

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
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1,
                              ('# For each step provide a description, as well as software name,'
                               'version, parameters, if applicable.'), Style.INSTRUCTION)
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, '# Include additional steps, as necessary.', Style.INSTRUCTION)
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1

        # print data processing steps
        for dp in self.data_processing_global_dict:
            content = dp + " : " + self.data_processing_global_dict[dp]
            #logging.debug(" ++ global data processing %s : %s --" % (dp, content))            
            self.excel.write_cell(self.current_row_idx, 1, "data processing step", Style.FIELD)
            self.excel.write_cell(self.current_row_idx, 2, content)            
            self.current_row_idx += 1

        # print genome build
        if self.reference_genome_set:
            content = ', '.join(self.reference_genome_set)
            self.excel.write_cell(self.current_row_idx, 1, 'genome build', Style.FIELD)
            self.excel.write_cell(self.current_row_idx, 2, content)
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
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'PROCESSED DATA FILES', Style.SECTION)
        self.header_row_idx = self.current_row_idx + 1
        self.current_row_idx += 2

        # -- replicate level processed files
        for replicate_rid in sorted(self.replicate_processed_file_dict):
            for pf in self.replicate_processed_file_dict[replicate_rid]:  #.sort(key = lambda x: x.get('File_Name')):
                local_col_idx = 1
                file_path = self.parameters.get("replicate_files_path_template", pf.get("File_Name", ""))
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

        # -- study level file. GEO doesn't want study-level processed files in this section
        # -- leave this false for now.
        if self.INCLUDE_STUDY_FILES_IN_PROCESSED_FILES_SECTION:
            for pf in self.study_processed_files:
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
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
            self.excel.write_cell(self.current_row_idx, col, '', Style.INSTRUCTION)
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 1, 'RAW FILES', Style.SECTION)
        self.header_row_idx = self.current_row_idx + 1
        self.current_row_idx += 2

        for replicate_rid in sorted(self.replicate_raw_file_dict):
            es = self.experiment_setting_dict[self.replicate_dict[replicate_rid]["Experiment_RID"]]
            for pf in sorted(self.replicate_raw_file_dict[replicate_rid], key = lambda x: x.get('File_Name')):
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
                self.excel.write_cell(self.header_row_idx, current_col_idx, 'instrument model', Style.HEADER)
                self.excel.write_cell(self.current_row_idx, current_col_idx, es.get('Sequencing_Platform', ''))
                current_col_idx += 1
                self.excel.write_cell(self.header_row_idx, current_col_idx, 'single or paired-end', Style.HEADER)
                self.excel.write_cell(self.current_row_idx, current_col_idx, pf.get('Paired_End', ''))
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
        for col in range(2, self.MAX_SECTION_COLUMN_IDX):
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
