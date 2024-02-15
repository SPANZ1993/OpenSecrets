import os
import time
import warnings
import traceback
import datetime
import itertools
import pandas as pd
from crpapi import CRP


class DataLoader:

    def __init__(self, data_path, api_key, cycles=None, states='All', politicians='All', wait_time=5):
        self.data_path = data_path
        self.api_key = api_key
        self.crp = CRP(api_key)

        if cycles is None:
            self.cycles = [self.latest_election_cycle()]
        else:
            self.cycles = cycles

        self.politicians = politicians

        self.default_wait_time = wait_time
        self.wait_time = wait_time

        self.states_csv_path = os.path.join(data_path, 'States.csv')
        print("States: ", self.states_csv_path, os.path.exists(self.states_csv_path))
        self.states_df = (pd.read_csv(self.states_csv_path)
                          if os.path.exists(self.states_csv_path) else None)
        print(self.states_df)
        if (self.states_df is None
                and (isinstance(states, str) and states == 'All')):
            raise ValueError("Cannot load full list of states from disk")
        if isinstance(states, str) and states == 'All':
            self.states = self.states_df['code'].values
        else:
            self.states = states

        self.politicians_csv_path = os.path.join(data_path, 'Politicians.csv')
        self.politicians_df = (pd.read_csv(self.politicians_csv_path)
                               if os.path.exists(self.politicians_csv_path) else None)

        self.sectors_csv_path = os.path.join(data_path, 'Sectors.csv')
        self.sectors_df = (pd.read_csv(self.sectors_csv_path)
                           if os.path.exists(self.sectors_csv_path) else None)

        if not os.path.exists(self.data_path):
            os.makedirs(data_path)

    def latest_election_cycle(self):
        y = datetime.datetime.now().year
        while y % 4 != 0:
            y -= 1
        return y

    def wrap_call(self, call_func):
        try:
            time.sleep(self.wait_time)
            result = call_func()
            self.wait_time = self.default_wait_time
            return result
        except Exception as e:
            self.wait_time = self.wait_time * 2.0
            raise e

    def politicians_info_to_df(self, candidate_info):
        expected_fields = [
            "cid",
            "firstlast",
            "lastname",
            "party",
            "office",
            "gender",
            "firstelectoff",
            "exitcode",
            "comments",
            "phone",
            "fax",
            "website",
            "webform",
            "congress_office",
            "bioguide_id",
            "votesmart_id",
            "feccandid",
            "twitter_id",
            "youtube_url",
            "facebook_id",
            "birthdate"
        ]
        # If there's only one candidate then a dict is returned
        if isinstance(candidate_info, dict):
            candidate_info = [candidate_info]

        rows = []
        for c in candidate_info:
            rows.append(pd.Series(c['@attributes'], index=expected_fields))

        df = pd.DataFrame(rows)
        return df

    def sector_info_to_df(self, sector_info, cid, cycle):
        expected_fields = [
            "sector_name",
            "sectorid",
            "indivs",
            "pacs",
            "total"
        ]
        rows = []
        for c in sector_info:
            d = c['@attributes']
            d.update({'candidate_id': cid, 'cycle': cycle})
            rows.append(pd.Series(d, index=['candidate_id', 'cycle'] + expected_fields))
        df = pd.DataFrame(rows)
        return df

    def load_state_politicians(self, state_code):
        if (self.politicians_df is None
                or not any(self.politicians_df['office'].str.startswith(state_code))):
            try:
                cur_politicians = self.wrap_call(lambda: self.crp.candidates.get(state_code))
                if (state_code == "DC"):
                    pass
            except:
                warnings.warn(f"Could not load politician info for state {state_code}")
                traceback.print_exc()
                return
            if len(cur_politicians) != 0:
                cur_politicians_df = self.politicians_info_to_df(cur_politicians)
            if self.politicians_df is None:
                self.politicians_df = cur_politicians_df
            else:
                self.politicians_df = pd.concat([self.politicians_df, cur_politicians_df], axis=0)

            self.politicians_df.to_csv(self.politicians_csv_path, index=0)
        else:
            print("SKIP")

    def load_politician_sectors(self, cid, cycle):
        if (self.sectors_df is None
                or not any((self.sectors_df['candidate_id'] == cid)
                           & (self.sectors_df['cycle'] == cycle))):
            try:
                cur_sectors = self.wrap_call(lambda: self.crp.candidates.sector(cid=cid, cycle=cycle))
            except:
                warnings.warn(f"Could not load sector info for candidate id {cid} for cycle {cycle}")
                traceback.print_exc()
                return
            if len(cur_sectors) != 0:
                cur_sectors_df = self.sector_info_to_df(cur_sectors, cid=cid, cycle=cycle)
            if self.sectors_df is None:
                self.sectors_df = cur_sectors_df
            else:
                self.sectors_df = pd.concat([self.sectors_df, cur_sectors_df], axis=0)

            self.sectors_df.to_csv(self.sectors_csv_path, index=0)
        else:
            print("SKIP")

    def run(self, silent=False):
        print("---------------- LOADING STATE POLITICIANS DATA ------------------")
        for i, state in enumerate(self.states):
            if not silent:
                print(f"{i + 1} / {len(self.states)}: Pulling politician data for state {state}")
            self.load_state_politicians(state)
        print('\n\n')
        print("---------------- LOADING INDUSTRY DATA ------------------")
        if (isinstance(self.politicians, str)
                and self.politicians == 'All'):
            cycles_politicians_prod = list(itertools.product(self.cycles, self.politicians_df['cid']))
        else:
            cycles_politicians_prod = list(itertools.product(self.cycles, self.politicians))

        for i, x in enumerate(cycles_politicians_prod):
            cycle = x[0]
            cid = x[1]
            if not silent:
                print(f"{i + 1} / {len(cycles_politicians_prod)}: Pulling data for {cid} in the {cycle} cycle")
            self.load_politician_sectors(cid=cid, cycle=cycle)
