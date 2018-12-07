import os
import json
import importlib
import logging
from copy import copy
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.transform.base_transform_processor import BaseTransformProcessor, \
    LOCAL_PATH_KEY, SOURCE_URL_KEY


class GeoExportTransformProcessor(BaseTransformProcessor):
    def __init__(self, envars=None, **kwargs):
        super(GeoExportTransformProcessor, self).__init__(envars, **kwargs)
        self._create_input_output_paths()
        self.excel_template_path = self.parameters.get("excel_template_path", "GEO_Metadata_Template.xlsx")

    def process(self):
        logging.info("Transforming input file [%s] into output file [%s] using template file [%s]" %
                     (self.input_abspath, self.output_abspath, self.excel_template_path))
        geo = Export2GEO(self.input_abspath, self.excel_template_path, self.output_abspath)
        geo.export_all()

        super(GeoExportTransformProcessor, self).process()
        return self.outputs


class Export2Excel(object):
    OPENPYXL = None

    def __init__(self, template_path, output_path):
        self.__name__ = 'export_geo_template'
        self.wb = None
        self.ws = None
        self._template_path_ = template_path
        self._output_path_ = output_path
        self.sheet_name = 'METADATA TEMPLATE'
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
        # Open an xlsx for reading
        self.wb = self.OPENPYXL.load_workbook(filename=self._template_path_)
        self.ws = self.wb[self.sheet_name]

    def cell_name(self, row_num, col_num):
        string = ""
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            string = chr(65 + remainder) + string
        return string + str(row_num)

    def insert_row(self, row_idx, row_amt):
        self.ws.insert_rows(idx=row_idx, amount=row_amt)

    def delete_row(self, idx, row_amt):
        self.ws.delete_rows(idx=idx, amount=row_amt)

    def write_cell(self, row_idx, col_idx, content):
        cell_loc = self.cell_name(row_idx, col_idx)
        cell = self.ws[cell_loc]
        cell.value = content

    def save_xlsx(self):
        self.wb.save(self._output_path_)

    def copy_cell(self, old_row_idx, old_col_idx, new_row_idx, new_col_idx,copy_style=True):
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
    def __init__(self, input_file, excel_template, output_excel_path):
        self.__name__ = 'export_geo_template'
        self.input_file = input_file
        self.excel_template = excel_template
        self.output_excel_path = output_excel_path
        self.study = None
        self.experiments = None
        self.experiment_settings = None
        self.replicates = None
        self.specimen = None
        self.files = None
        self.study_files = None
        self.excel = None
        self.current_row_idx = 0
        self.header_row_idx = 0
        self.rows_to_delete = []
        self.protocol_type = ['Isolation_Protocol', 'Growth_Protocol', 'Treatment_Protocol', 'Extract_Protocol',
                              'Construction_Protocol', 'Label_Protocol', 'Hybridization_Protocol', 'Scan_Protocol']
        self.protocol_unique = {}
        self.protocol_list = {}
        self.other_item = ['Data_Processing', 'Reference_Genome']
        self.other_item_unique = {}
        self.other_item_list = {}
        self.initialize_data()

    def initialize_data(self):
        with open(self.input_file) as input_file:
            data = json.load(input_file)
            if len(data) == 0:
                logging.debug("No Data")
            elif len(data) > 1:
                logging.debug("Duplicate Data")
            elif len(data) == 1:
                self.study = data[0].get('Study')[0]
                self.experiments = data[0].get('Experiment')
                self.experiment_settings = data[0].get('Experiment_Setting')
                self.replicates = data[0].get('Replicate')
                self.specimen = data[0].get('Specimen')
                self.files = data[0].get('File')
                self.study_files = data[0].get('Study_File')

    def export_start(self):
        self.excel = Export2Excel(self.excel_template, self.output_excel_path)

    def export_all(self):
        if self.study is not None:
            self.export_start()
            self.export_series()
            self.export_sample_prepare()
            self.export_sample()
            self.export_protocol()
            self.export_data_processing()
            self.export_processed_datafiles()
            self.export_raw_datafiles()
            self.export_paired_end()
            self.export_finish()

    def export_series(self):
        # write SERIES
        self.current_row_idx = 9
        self.excel.write_cell(self.current_row_idx, 2, self.study.get('Title',''))
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 2, self.study.get('Summary',''))
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 2, self.study.get('Overall_Design',''))
        self.current_row_idx += 1
        self.excel.write_cell(self.current_row_idx, 2, self.study.get('Principal_Investigator_Name',''))
        self.current_row_idx += 1

        if len(self.study_files) >= 1 and self.study_files[0] is not None:
            self.excel.write_cell(self.current_row_idx, 2, self.study_files[0].get('Name',''))
            self.current_row_idx += 1
            for i in range(len(self.study_files) - 1):
                self.excel.insert_row(self.current_row_idx, 1)
                self.excel.copy_cell(self.current_row_idx - 1, 1, self.current_row_idx, 1)
                self.excel.write_cell(self.current_row_idx, 2, self.study_files[i + 1].get('Name',''))
                self.current_row_idx += 1
        else:
            self.excel.write_cell(self.current_row_idx, 2, '')
            logging.debug('No study file')

    def export_sample_prepare(self):
        # check self.protocol unique
        for p in self.protocol_type:
            # protocols which are applicable to only a subset of Samples should be included
            # as additional columns of the SAMPLES section instead.
            # this dictionary is used to handle dynamic headers related to protocol
            self.protocol_unique[p] = True
            self.protocol_list[p] = []

        for i in self.other_item:
            self.other_item_unique[i] = True
            self.other_item_list[i] = []

        for e in self.experiments:
            for p in self.protocol_type:
                if len(self.protocol_list[p]) == 0 and e[p] is not None and e[p] != 'None':
                    self.protocol_list[p].append(e[p])
                elif e[p] not in self.protocol_list[p] and e[p] is not None and e[p] != 'None':
                    self.protocol_list[p].append(e[p])
                    self.protocol_unique[p] = False
            i = 'Data_Processing'
            if i in e.keys():
                if len(self.other_item_list[i]) == 0 and e[i] is not None and e[i] != 'None':
                    self.other_item_list[i].append(e[i])
                elif e[i] not in self.other_item_list[i] and e[i] is not None and e[i] != 'None':
                    self.other_item_list[i].append(e[i])
                    self.other_item_unique[i] = False

        for e in self.experiment_settings:
            i = 'Reference_Genome'
            if i in e.keys():
                if len(self.other_item_list[i]) == 0 and e[i] is not None and e[i] != 'None':
                    self.other_item_list[i].append(e[i])
                elif e[i] not in self.other_item_list[i] and e[i] is not None and e[i] != 'None':
                    self.other_item_list[i].append(e[i])
                    self.other_item_unique[i] = False

    def export_sample(self):
        self.header_row_idx = self.current_row_idx + 6
        self.current_row_idx = self.current_row_idx + 6
        # write SAMPLES
        for e in self.experiments:
            if e is None or 'RID' not in e.keys():
                continue
            for r in self.replicates:
                if r is None or 'Experiment_RID' not in r.keys() or 'RID' not in r.keys():
                    continue
                elif r['Experiment_RID'] == e['RID']:
                    local_col_idx = 1
                    self.current_row_idx += 1
                    self.excel.insert_row(self.current_row_idx, 1)
                    sample_name = e.get('Internal_ID','') + '_' + str(r.get('Biological_Replicate_Number','')) + '_' + str(
                        r.get('Technical_Replicate_Number',''))
                    sample_title = e.get('Name','') + '_' + str(r.get('Biological_Replicate_Number','')) + '_' + str(
                        r.get('Technical_Replicate_Number',''))
                    sample_source_name = e.get('Anatomy','')
                    sample_organism = e.get('Species','')
                    sample_molecule = e.get('Molecule_Type','')

                    self.excel.write_cell(self.current_row_idx, local_col_idx, sample_name)
                    local_col_idx += 1
                    self.excel.write_cell(self.current_row_idx, local_col_idx, sample_title)
                    local_col_idx += 1
                    self.excel.write_cell(self.current_row_idx, local_col_idx, sample_source_name)
                    local_col_idx += 1
                    self.excel.write_cell(self.current_row_idx, local_col_idx, sample_organism)
                    local_col_idx += 1

                    for p in self.protocol_type:
                        if self.protocol_unique[p] == False:
                            self.excel.copy_cell(self.header_row_idx - 1, 11, self.header_row_idx, local_col_idx)
                            self.excel.write_cell(self.header_row_idx, local_col_idx, (p.replace('_', ' ').lower()))
                            self.excel.write_cell(self.current_row_idx, local_col_idx, e[p])
                            local_col_idx += 1

                    # todo list all the characteristics properties
                    for s in self.specimen:
                        if s is None or 'REPLICATE_RID' not in s.keys():
                            continue
                        elif s['REPLICATE_RID'] == r['RID']:
                            sample_phenotype = s.get('Phenotype','')
                            self.excel.copy_cell(self.header_row_idx - 1, 5, self.header_row_idx, local_col_idx)
                            self.excel.write_cell(self.header_row_idx, local_col_idx, 'characteristics: Phenotype')
                            self.excel.write_cell(self.current_row_idx, local_col_idx, sample_phenotype)
                            local_col_idx += 1
                        else:
                            continue

                    # copy header
                    self.excel.copy_cell(self.header_row_idx - 1, 7, self.header_row_idx, local_col_idx)
                    self.excel.write_cell(self.current_row_idx, local_col_idx, sample_molecule)
                    local_col_idx += 1

                    for s in self.specimen:
                        if s is None or 'REPLICATE_RID' not in s.keys():
                            continue
                        elif s['REPLICATE_RID'] == r['RID']:
                            self.excel.copy_cell(self.header_row_idx - 1, 8, self.header_row_idx, local_col_idx)
                            sample_description = s.get('Upload_Notes','')
                            self.excel.write_cell(self.current_row_idx, local_col_idx, sample_description)
                            local_col_idx += 1
                        else:
                            continue
                    # writing processed file
                    # todo: how to handle multi processed files for one sample

                    # writing raw file
                    for f in self.files:
                        if f is None or 'Replicate_RID' not in f.keys():
                            continue
                        elif f['Replicate_RID'] == r['RID']:
                            self.excel.copy_cell(self.header_row_idx - 1, 10, self.header_row_idx, local_col_idx)
                            self.excel.write_cell(self.current_row_idx, local_col_idx, f.get('File_Name',''))
                            local_col_idx += 1
                        else:
                            continue

                    for i in [1,2,3,4,5,6,7,8]:
                        self.excel.copy_cell(self.header_row_idx - 2, local_col_idx, self.header_row_idx, local_col_idx,False)
                        local_col_idx += 1
                else:
                    continue

        self.rows_to_delete.insert(0, self.header_row_idx - 1)
        self.rows_to_delete.insert(0, self.current_row_idx + 1)

    def export_protocol(self):
        self.header_row_idx = self.current_row_idx + 5
        self.current_row_idx = self.current_row_idx + 6

        if self.protocol_unique['Growth_Protocol']:
            if len(self.protocol_list['Growth_Protocol']) == 1:
                self.excel.write_cell(self.current_row_idx, 2, self.protocol_list['Growth_Protocol'][0])
            else:
                self.excel.write_cell(self.current_row_idx, 2, '')
        else:
            self.excel.write_cell(self.current_row_idx, 2, '')
        self.current_row_idx += 1

        if self.protocol_unique['Treatment_Protocol']:
            if len(self.protocol_list['Treatment_Protocol']) == 1:
                self.excel.write_cell(self.current_row_idx, 2, self.protocol_list['Treatment_Protocol'][0])
            else:
                self.excel.write_cell(self.current_row_idx, 2, '')
        else:
            self.excel.write_cell(self.current_row_idx, 2, '')
        self.current_row_idx += 1

        if self.protocol_unique['Extract_Protocol']:
            if len(self.protocol_list['Extract_Protocol']) == 1:
                self.excel.write_cell(self.current_row_idx, 2, self.protocol_list['Extract_Protocol'][0])
            else:
                self.excel.write_cell(self.current_row_idx, 2, '')
        else:
            self.excel.write_cell(self.current_row_idx, 2, '')
        self.current_row_idx += 1

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
                if self.protocol_unique[p] and len(self.protocol_list[p]) == 1:
                    self.excel.insert_row(self.current_row_idx, 1)
                    self.excel.copy_cell(self.header_row_idx, 1, self.current_row_idx, 1)
                    self.excel.write_cell(self.current_row_idx, 1, (p.replace('_', ' ').lower()))
                    self.excel.write_cell(self.current_row_idx, 2, self.protocol_list[p][0])
                    self.current_row_idx += 1

        self.excel.write_cell(self.current_row_idx, 2, 'RNA-seq')
        self.current_row_idx += 1

    def export_data_processing(self):
        # DATA PROCESSING PIPLINE
        self.header_row_idx = self.current_row_idx + 5
        self.current_row_idx = self.current_row_idx + 6

        if len(self.other_item_list['Data_Processing']) == 1:
            processing_list = self.other_item_list['Data_Processing'][0].split('\n')
        else:
            processing_list = []
        for step_row in processing_list:
            self.excel.insert_row(self.current_row_idx, 1)
            self.excel.copy_cell(self.header_row_idx, 1, self.current_row_idx, 1)
            self.excel.write_cell(self.current_row_idx-1, 2, step_row)
            self.current_row_idx += 1

        # remove the unused row
        if len(processing_list)>=1:
            self.rows_to_delete.insert(0, self.current_row_idx - 1)

        if self.other_item_unique['Reference_Genome']:
            if len(self.other_item_list['Reference_Genome']) == 1:
                self.excel.write_cell(self.current_row_idx, 2, self.other_item_list['Reference_Genome'][0])
            else:
                self.excel.write_cell(self.current_row_idx, 2, ' '.join(self.other_item_list['Reference_Genome']))
                logging.debug('Multi-genome')

        self.current_row_idx += 1

        # todo: where to find data
        self.excel.write_cell(self.current_row_idx, 2, 'processed data file format')
        self.current_row_idx += 1

    def export_processed_datafiles(self):
        # PROCESSED DATA FILES
        self.current_row_idx = self.current_row_idx + 5
        self.rows_to_delete.insert(0, self.current_row_idx - 1)
        for pf in self.files:
            if pf is None:
                continue
            else:
                local_col_idx = 1
                self.excel.insert_row(self.current_row_idx, 1)
                self.excel.write_cell(self.current_row_idx, local_col_idx, pf.get('File_Name',''))
                local_col_idx += 1
                self.excel.write_cell(self.current_row_idx, local_col_idx, pf.get('File_Type',''))
                local_col_idx += 1
                self.excel.write_cell(self.current_row_idx, local_col_idx, pf.get('MD5',''))
                self.current_row_idx += 1

    def export_raw_datafiles(self):
        # RAW FILES
        self.current_row_idx = self.current_row_idx + 5
        self.rows_to_delete.insert(0, self.current_row_idx - 1)
        self.rows_to_delete.insert(0, self.current_row_idx)

        for pf in self.files:
            if pf is None or 'Experiment_RID' not in pf.keys():
                continue
            else:
                current_col_idx = 1
                self.excel.insert_row(self.current_row_idx, 1)
                self.excel.write_cell(self.current_row_idx, current_col_idx, pf.get('File_Name',''))
                current_col_idx += 1
                self.excel.write_cell(self.current_row_idx, current_col_idx, pf.get('File_Type',''))
                current_col_idx += 1
                self.excel.write_cell(self.current_row_idx, current_col_idx, pf.get('MD5',''))
                current_col_idx += 1
                # Experiment_Settings.Protocol_Reference
                for es in self.experiment_settings:
                    if es is None or 'Experiment_RID' not in es.keys():
                        continue
                    elif es['Experiment_RID'] == pf['Experiment_RID']:
                        self.excel.write_cell(self.current_row_idx, current_col_idx, es.get('Sequencing_Platform',''))
                        current_col_idx += 1
                        self.excel.write_cell(self.current_row_idx, current_col_idx, es.get('Read_Length',''))
                        current_col_idx += 1
                        if 'pair' in str(es.get('Paired_End','')).lower():
                            single_or_paired = 'paired-end'
                        else:
                            single_or_paired = 'single'
                        self.excel.write_cell(self.current_row_idx, current_col_idx, single_or_paired)
                        current_col_idx += 1
        self.excel.insert_row(self.current_row_idx, 1)

    def export_paired_end(self):
        # PAIRED-END EXPERIMENTS
        # todo need check what's paired-end data look like
        self.current_row_idx = self.current_row_idx + 5
        self.header_row_idx = self.current_row_idx + 4

    def export_finish(self):
        for d in self.rows_to_delete:
            self.excel.delete_row(d, 1)
        self.excel.save_xlsx()




