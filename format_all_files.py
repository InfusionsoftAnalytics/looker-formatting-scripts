import re
import shutil
import os
import numpy as np
import pandas as pd
import sys

class LookerFormatter():

  def __init__(self, files=[]):
    self.views = []

    self.intro = ("### sql_table_name refers to the dbt-produced table in BigQuery that serves this derived table.\r\n" + 
  "### You may view the schema and underlying SQL at https://storage.googleapis.com/dbt-docs/index.html \r\n" +
  "### If you wish to add to or filter this table, you may do so by commenting out the `sql_table_name` declaration \r\n" +
  "### and instead calling the table in a derived_table block, together with whatever joins or filters, as shown below:\r\n" +
  "### \r\n" + "###   derived_table { \r\n" + "###      sql: SELECT * FROM analytics.TABLE_NAME [perform joins or filters here] ;; \r\n" +
  "###      sql_trigger_value: SELECT * FROM analytics.TABLE_NAME ;; \r\n" + "###   } \r\n" + "### \r\n" +
  "### Please do not pull the full table SQL into this file, as it will slow down Looker! \r\n\n"
  )

    self.key = ("\r\n####################################################################################################################\r\n" +
  "################################################ Primary Key ########################################################\r\n" + 
  "####################################################################################################################\r\n"
  )

    self.dimension = ("\r\n####################################################################################################################\r\n" + 
  "################################################ Dimensions ########################################################\r\n" + 
  "####################################################################################################################\r\n"
  )

    self.measure = ("\r\n####################################################################################################################\r\n" + 
  "################################################ Measures ########################################################\r\n" + 
  "####################################################################################################################\r\n"
  )

    self.field_set = ("\r\n####################################################################################################################\r\n" + 
  "################################################ Field Sets ########################################################\r\n" + 
  "####################################################################################################################\r\n"
  )

    self.parameter = ("\r\n####################################################################################################################\r\n" + 
  "################################################ Parameters ########################################################\r\n" + 
  "####################################################################################################################\r\n"
  )

    self.filter_field = ("\r\n####################################################################################################################\r\n" + 
  "################################################ Filters ########################################################\r\n" + 
  "####################################################################################################################\r\n"
  )


  def clean_and_decompose_document(self,view_content):
    #split on line break or carriage return
    contents = re.split(r'\r|\n',view_content)

    #delete commented code
    view_content = "\n".join([field for field in contents if not re.search(r'^(\s?|\s\s+?)#.*',field)])

    #replace double carriage with single carriage
    view_content = re.sub(r'[\r\n]+[\r\n]',r'\r\n',view_content)

    #put something at beginning to parse
    view_content = re.sub(r'^','\r\n',view_content)

    #split on fields
    field_text = [i.strip() for i in re.split(r'\sinclude:|\sview:|\sdimension:|\sdimension_group:|\smeasure:|\sset:|\sparameter:|\sfilter:',view_content)]

    #save field types
    field_type = [i.strip() for i in re.findall(r'\sinclude:|\sview:|\sdimension:|\sdimension_group:|\smeasure:|\sset:|\sparameter:|\sfilter:',view_content)]

    #strip trailing bracket from last element
    field_text[-1] = field_text[-1].rstrip().rstrip('}').strip()

    #strip leading element from field text
    field_text = field_text[1:]

    return field_text, field_type

  def process_views(self, directory = ""):
    base_views = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith("__base.view.lkml")]
    print('number of base views to process:',len(base_views))

    field_set_views = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith("__field_set.view.lkml")]
    print('number of field set views to process:',len(field_set_views))

    for view_file in base_views:
      print("Processing " + view_file)

      view_content = open(view_file, encoding='utf-8').read()
      field_text, field_type = self.clean_and_decompose_document(view_content)
      generated_doc = self.generate_base_doc(field_text,field_type)
      self.write_new_view(view_file,generated_doc,in_place=True)

    for view_file in field_set_views:
      print("Processing " + view_file)
      
      view_content = open(view_file, encoding='utf-8').read()
      field_text, field_type = self.clean_and_decompose_document(view_content)
      generated_doc =  self.generate_field_set_doc(field_text,field_type)
      self.write_new_view(view_file,generated_doc,in_place=True)

  def generate_field_set_doc(self,field_text,field_type):
    inclusions_list, field_text, field_type = self.generate_field_set_field(field_text,field_type,'include')
    view_definition, field_text, field_type = self.generate_field_set_field(field_text,field_type,'view')
    key_inclusion, field_text, field_type  = self.generate_field_set_field(field_text,field_type,'key')
    dimensions, field_text, field_type = self.generate_field_set_field(field_text,field_type,'dimension')
    measures, field_text, field_type = self.generate_field_set_field(field_text,field_type,'measure')
    field_sets, field_text, field_type = self.generate_field_set_field(field_text,field_type,'set')
    parameter_fields, field_text, field_type = self.generate_field_set_field(field_text,field_type,'parameter')
    filter_fields, field_text, field_type = self.generate_field_set_field(field_text,field_type,'filter')

    print("Leftover field_text: ")
    for field, typ in zip(field_text,field_type):
      print(' ' + typ + ' ' + field)
    
    #put in alphabetical order
    inclusions_list.sort()
    dimensions.sort()
    measures.sort()
    parameter_fields.sort()

    return (inclusions_list + view_definition + [self.key] + key_inclusion + [self.dimension] + dimensions + 
                  [self.measure] + measures + [self.parameter] + parameter_fields)

  def generate_field_set_field(self,field_text,field_type,category):
    results_array = []
    leftover_field_type = []
    leftover_field_text = []

    for field, typ in zip(field_text,field_type):
      if  "key" in category and "primary_key: yes" in field: 
        results_array.append(' ' + typ + ' ' + field)
      else: 
        if category in typ:
          field = re.sub(r'hidden: yes',r'hidden: no',field)
          field = re.sub(r'hidden:  yes',r'hidden: no',field)

          if category in ('parameter','measure','dimension') and 'hidden:' not in field:
            field = re.sub(r'}$',r'\thidden: no\r\n\t}',field)
          results_array.append(' ' + typ + ' ' + field)
        else:
          leftover_field_type.append(typ)
          leftover_field_text.append(field)
      
    return results_array, leftover_field_text, leftover_field_type

  def generate_base_doc(self,field_text,field_type,hidden = False):
    inclusions_list, field_text, field_type = self.generate_base_field(field_text,field_type,'include')
    view_definition, field_text, field_type = self.generate_base_field(field_text,field_type,'view')
    key_inclusion, field_text, field_type  = self.generate_base_field(field_text,field_type,'key')
    dimensions, field_text, field_type = self.generate_base_field(field_text,field_type,'dimension')
    measures, field_text, field_type = self.generate_base_field(field_text,field_type,'measure')
    field_sets, field_text, field_type = self.generate_base_field(field_text,field_type,'set')
    parameter_fields, field_text, field_type = self.generate_base_field(field_text,field_type,'parameter')
    filter_fields, field_text, field_type = self.generate_base_field(field_text,field_type,'filter')

    print("Leftover field_text: ")
    for field, typ in zip(field_text,field_type):
      print(' ' + typ + ' ' + field)
    
    #put in alphabetical order

    inclusions_list.sort()
    dimensions.sort()
    measures.sort()
    parameter_fields.sort()
    field_sets.sort()
    filter_fields.sort()

    return ([self.intro] + inclusions_list + view_definition + [self.key] + key_inclusion + [self.dimension] + dimensions + 
                  [self.measure] + measures + [self.field_set] + field_sets + [self.parameter] + parameter_fields + 
                  [self.filter_field] + filter_fields)

  def generate_base_field(self,field_text,field_type,category):
    results_array = []
    leftover_field_type = []
    leftover_field_text = []

    for field, typ in zip(field_text,field_type):
      if  "key" in category and "primary_key: yes" in field:
        results_array.append(' ' + typ + ' ' + field)
      else: 
        if category in typ:
          field = re.sub(r'hidden: no',r'hidden: yes',field)
          field = re.sub(r'hidden:  no',r'hidden: yes',field)

          if category in ('parameter','measure','dimension') and 'hidden:' not in field:
            field = re.sub(r'}$',r'\thidden: yes\r\n\t}',field)
          results_array.append(' ' + typ + ' ' + field)
        else:
          leftover_field_type.append(typ)
          leftover_field_text.append(field)
      
    return results_array, leftover_field_text, leftover_field_type

  def write_new_view(self,file,generated_doc,in_place=True):
    f = open(file,"w+")

    for r in generated_doc:
        f.write(r)
        f.write('\n')

    f.write('}')
    f.close()

formatter=LookerFormatter()
formatter.process_views(directory = "/Users/hannahburak/Documents/Infusionsoft/IS_looker/")

#To Do: build in contingency for convenience tables; try to make field sets pick up THE KEY DIMENSION from the base VIEWS; make sure analyst all view contain all fields