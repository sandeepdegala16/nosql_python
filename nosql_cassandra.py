import pandas as pd

#Assuming both the input files are TSV(Tab-Separated Values)
def cliparser(nodetool_file, mapperhost_file):
    
    cassandra_ip_df = pd.read_csv(nodetool_file,sep='\t')
    mapper_host_ip = pd.read_csv(mapperhost_file,sep='\t')
    
    output = []
    col = ['Rack','Hostname','HostID','Load']
    
    for i in cassandra_ip_df['Address']:
        for j in mapper_host_ip['Address']:
            if i == j:
                match = []
                #getting the required values for each match
                match.extend(list(cassandra_ip_df.loc[cassandra_ip_df['Address']==i]['Rack']))
                match.extend(list(mapper_host_ip.loc[mapper_host_ip['Address']==i]['Hostname']))
                match.extend(list(cassandra_ip_df.loc[cassandra_ip_df['Address']==i]['HostID']))
                match.extend(list(cassandra_ip_df.loc[cassandra_ip_df['Address']==i]['Load']))
                output.append(match)

    return(pd.DataFrame(output,columns=col))

output_df = cliparser('nodetool_ip.txt','mapper_host.txt')

#Any of the below 2 outputs can be used for the given requirement

#1. Output based on the given Rack input value
print(output_df.loc[output_df['Rack']=='1c'][['Rack','Hostname','HostID','Load']])
print(output_df.loc[output_df['Rack']=='1a'][['Rack','Hostname','HostID','Load']])

#2. Generated output by grouping the Rack values
output_df.groupby(by='Rack').apply(print)
