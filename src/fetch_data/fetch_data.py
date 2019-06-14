import pandas as pd
from tqdm import tqdm

from fantasyprem.api_connectors.player_summary import PlayerSummaryConnection
from fantasyprem.api_connectors.player import PlayerConnection
from fantasyprem.api_connectors.team import TeamConnection

from .functions import attach_timestamp
from .functions import sql_append_new_rows
from .functions import write_with_backup


def fetch_player_summary_data():
	"""
	Fetches all data from the fantasy football API and writes to the MySQL database
	"""
	# Read and insert player summary
	summary_table = PlayerSummaryConnection().get_content(True)
	attach_timestamp(summary_table)
	write_with_backup(summary_table, "player_summary", "player_summary_history", "fantasy_football")


def fetch_player_history_data(id_list):
	# Read and insert player history
	player_table = pd.DataFrame()
	for i in tqdm(id_list):
		player_table = player_table.append(PlayerConnection(i).get_history(True))
	attach_timestamp(player_table)
	# Only write new rows
	sql_append_new_rows(player_table, "player_history", key=["id"], database="fantasy_football")


def fetch_team_list():
	team_table = TeamConnection().get_content(True)
	team_table = team_table.where(~team_table.applymap(lambda x: x == [] or x is None))
	# ToDo: decide best place for this cleaning above
	attach_timestamp(team_table)
	sql_append_new_rows(team_table, "teams", key=["current_event_fixture"], database="fantasy_football")
