import pandas as pd
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    '../credentials/data-practice-348620-b8edf47bbba1.json')

project_from = 'bigquery-public-data'
project_to = 'data-practice-348620'


dataset_from = 'ncaa_basketball'
dataset_to = 'basketball'

tables = ['mascots', 'mbb_games_sr', 'mbb_historical_teams_games',
          'mbb_historical_teams_seasons', 'mbb_historical_tournament_games',
          'mbb_pbp_sr', 'mbb_players_games_sr', 'mbb_teams', 'mbb_teams_games_sr', 'team_colors']
for table in tables:
    print(table)
    df_from = pd.read_gbq(
        f'select * from {project_from}.{dataset_from}.{table}', project_to, credentials=credentials)
    df_from.to_gbq(f'{dataset_to}.{table}', project_to, if_exists='replace',
                   credentials=credentials)
