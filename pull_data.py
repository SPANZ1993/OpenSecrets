import toml
import os
import pandas as pd
from dataloader import DataLoader


def get_configs():
    with open('config.toml', 'r') as f:
        d = toml.load(f)
    return d

def get_secrets():
    with open('secret.toml', 'r') as f:
        d = toml.load(f)
    return d

def get_cycles(start, end):
    return [x for x in range(start, end+1) if x % 4 == 0]


def build_sankey_df(states_df, politicians_df, sectors_df):
    politicians_df['state_abbr'] = politicians_df['office'].str[:2]

    joined = (politicians_df
              .merge(states_df, how='left', left_on='state_abbr', right_on='code')
              .merge(sectors_df, how='left', left_on='cid', right_on='candidate_id'))

    joined2 = joined.copy(deep=True)
    joined['VizSide'] = 'Sector'
    joined2['VizSide'] = 'Politician'

    return pd.concat([joined, joined2])



def dfs_tabs(df_list, sheet_list, file_name):
    # https://stackoverflow.com/questions/32957441/putting-many-python-pandas-dataframes-to-one-excel-worksheet
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe.to_excel(writer, sheet_name=sheet, startrow=0, startcol=0)
    writer._save()

def main():
    configs = get_configs()
    secrets = get_secrets()

    API_KEY = secrets['API_KEY']
    DATA_DIR = secrets['DATA_DIR']
    if not os.path.exists(DATA_DIR):
        raise OSError(f'Data Directory {DATA_DIR} does not exist')

    cycles = get_cycles(configs['data_to_pull']['cycles']['start'],
                        configs['data_to_pull']['cycles']['end']
                        )
    states = configs['data_to_pull']['states']
    politicians = configs['data_to_pull']['politicians']

    DL = DataLoader(DATA_DIR, API_KEY, cycles=cycles, states=states, politicians=politicians)
    DL.run()


    states_df = pd.read_csv(os.path.join(DATA_DIR, 'States.csv'))
    politicians_df = pd.read_csv(os.path.join(DATA_DIR, 'Politicians.csv'))
    sectors_df = pd.read_csv(os.path.join(DATA_DIR, 'Sectors.csv'))


    # We want to create a Sankey Diagram, and in order to do that we need a unioned version of our merged data

    sankey_df = build_sankey_df(states_df, politicians_df, sectors_df)

    dfs_tabs(df_list=[states_df, politicians_df, sectors_df, sankey_df],
             sheet_list=['States', 'Politicians', 'Sectors', 'Sankey'],
             file_name=os.path.join(DATA_DIR, 'Campaign_Data.xlsx'))


if __name__ == '__main__':
    main()