import os

# TODO: Command line argument
path = '/home/ec2-user/atul/csvtosql/'
delimiter =  ','
output_dir = ''
output_type = '' # SQL OR HQL
filenames_list = [path+x for x in os.listdir(path) if os.path.isfile(x)]
for file in filenames_list:
    with open(file, 'r') as current_file:
        # TODO: Validation for extension of CSV and TSV
        tbl_name, ext = os.path.splitext(file)
        first_line = current_file.readline()
        col_list = first_line.split(delimiter)
        s = "CREATE EXTERNAL TABLE IF NOT EXISTS " + tbl_name + "( \n"
        for name in col_list[:-1]:
            s += "\t`" + name.lower().replace(" ","-").replace('"', "") + '` STRING, \n'

        s += "\t`" +  col_list[-1].lower().replace("\n",'').replace("\r", '').replace(" ","-").replace('"', "") + '` STRING \n'
        s += ')\n'
        #TODO: Check whether output should be SQL or HQL
        s += 'ROW FORMAT DELIMITED \n'
        s += 'FIELDS TERMINATED BY \''+delimiter+'\' \n'
        s += "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' \n"
        s += "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \n"
        s += "LOCATION '/user/svc_dev_vera_wf/AI_STD/Pre-standardization/" + tbl_name + "/' \n"
        s += "TBLPROPERTIES ( \n \t 'serialization.null.format' = '', \n \t 'skip.header.line.count' = '1');"
        s += '\n\n'

        # TODO: Write the output file
        print(s)
