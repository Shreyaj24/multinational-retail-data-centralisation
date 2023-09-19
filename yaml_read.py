import yaml

with open('db_creds.yaml', 'r') as yaml_cred_file:
    cred_data = yaml.safe_load(yaml_cred_file)
    print(cred_data)
    #return cred_data