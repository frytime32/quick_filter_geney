from geneyquery import GeneyFileCollection
from geneyquery import GeneyQuery
import pandas as pd

files = GeneyFileCollection("Metadata.tsv", "Metadata.mp", "Metadata_transposed.tsv", "Metadata_transposed.mp")

gq = GeneyQuery(files,'{"filters":{"donor_age":[{"operator":">","value":50}],"base_cell_id":["MCF7","PC3"]},"features":["donor_age","donor_ethnicity"],"groups":["Metadata"]}')


#i = ['abc\t', 'def\t']
#print(i)
df = gq.filter_data()
df.to_csv("test_output.tsv", sep = "\t", index=False)
