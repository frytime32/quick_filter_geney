import sys
import msgpack
import io
import csv
import pandas as pd
###########################################################################################
# Opening all necessary files, maps, and lists that will be used

tsv_file_path = sys.argv[1]
messagepack_tsv_path = sys.argv[2]
transposed_tsv_file_path = sys.argv[3]
transposed_messagepack_tsv_path = sys.argv[4]
output_file_path = sys.argv[5]

messagepack_tsv = open(messagepack_tsv_path + "sample_data.msgpack", "rb")
tsv_map = msgpack.unpack(messagepack_tsv)

sample_file = open(messagepack_tsv_path + "samples.msgpack", "rb")
samples = msgpack.unpack(sample_file)

transposed_map_file = open(transposed_messagepack_tsv_path + "sample_data.msgpack", "rb")
transposed_map = msgpack.unpack(transposed_map_file)

transposed_samples_file = open(transposed_messagepack_tsv_path + "samples.msgpack", "rb")
transposed_samples = msgpack.unpack(transposed_samples_file)

tsv_file = open(tsv_file_path, "rb")
transposed_tsv_file = open(transposed_tsv_file_path, "rb")

tsv_headers = tsv_file.readline()
##############################################################################################


# MessagePack seems to store things as bytes, so most things need to be cast as bytes to be considered valid

# Identify samples that have a value of "MCF7" for the base_cell_id feature and a value greater than 50 for donor_age.
location_range1 = transposed_map[b"base_cell_id"]
transposed_tsv_file.seek(location_range1[0])
desired_feature_data1 = transposed_tsv_file.read(location_range1[1]).split(b"\t")

location_range2 = transposed_map[b"donor_age"]
transposed_tsv_file.seek(location_range2[0])
desired_feature_data2 = transposed_tsv_file.read(location_range2[1]).split(b"\t")

matching_samples = []
print("Finding samples that match the given filter...")
for i in range(0, len(desired_feature_data1)):
    if desired_feature_data1[i] == b"MCF7" and int(desired_feature_data2[i]) > 50:
        matching_samples.append(samples[i])

print("Obtaining row data for those matching samples")
output_rows = []
for sample in matching_samples:
    tsv_file.seek(tsv_map[sample][0])
    entire_row = tsv_file.read(tsv_map[sample][1])
    output_rows.append(entire_row)

print("Writing all data to the output file")
#output_file = open(output_file_path, "wb")
#output_file.write(tsv_headers)
output = io.StringIO()
csv_writer = csv.writer(output, delimiter="\t")
for row in output_rows:
    csv_writer.writerow(row)
output.seek(0)
df = pd.read_csv(output, sep='\t')
print(str(df.columns.values))
#output_file.write(b"\n".join(output_rows))
