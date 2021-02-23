import sqlite3

def get_results(dbname, table_name, where):
    conn=sqlite3.connect(dbname)
    cursor = conn.cursor()
    get_records = f"SELECT * FROM {table_name} {where}"
    cursor.execute(get_records)
    results = cursor.fetchall()
    conn.close()
    return results

RASE_COLS = ["race_id", "place", "race_number", "deadline", "distance", "title_name", "status", "create_at"]
TIMETABLE_RACER_COLS = [
    "time_table_racer_id",
    "race_id",
    "course",
    "racer_id",
    "name",
    "age",
    "weight",
    "rank",
    "win_rate",
    "exacta_rate",
    "win_rate_place",
    "exacta_race_place",
    "moter_id",
    "exacta_race_mortor",
    "boat_id",
    "exacta_race_boat",
    "result_1",
    "result_2",
    "result_3",
    "result_4",
    "result_5",
    "result_6",
    "exhibition_time",
    "tilt",
    "create_at"
]
RACER_RESULT_COLS = ["result_id", "time_table_racer_id", "time", "prize", "disqualification", "create_at"]

RANK_NUM = {'A1': 1, 'A2': 2, 'B1': 3, 'B2': 4}