from DbConnector import DbConnector
from tabulate import tabulate
import math
import os

from itertools import groupby

from util import Trajectory, LabelsActivity


class DatabaseUtil:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        query = """CREATE TABLE IF NOT EXISTS users (
                   id VARCHAR(225) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN NOT NULL DEFAULT FALSE
                   )
                """
        return query

    def create_activity_table(self):
        query = """CREATE TABLE IF NOT EXISTS activities (
                    id VARCHAR(225) NOT NULL PRIMARY KEY,
                    transportation_mode VARCHAR(120),
                    start_date_time DATETIME NOT NULL,
                    end_date_time DATETIME NOT NULL,
                    user_id VARCHAR(225) NOT NULL,
                    CONSTRAINT FK_UserActivity FOREIGN KEY (user_id) REFERENCES users(id)
                )
                """
        return query

    def create_track_point(self):
        query = """CREATE TABLE IF NOT EXISTS track_points (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    lat DOUBLE(20, 10),
                    lon DOUBLE(20, 10),
                    altitude INT(30),
                    date_time DATETIME NOT NULL,
                    activity_id VARCHAR(225) NOT NULL,
                    CONSTRAINT FK_ActivityTrackPoint FOREIGN KEY (activity_id) REFERENCES activities(id)
                    )
                """
        return query

    def drop_all(self):
        query = """
                DROP TABLE track_points;
                DROP TABLE activities;
                DROP TABLE users;
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def setup(self):
        queries = []
        queries.append(self.create_user_table())
        queries.append(self.create_activity_table())
        queries.append(self.create_track_point())

        self.cursor.execute(queries[0])
        self.cursor.execute(queries[1])
        self.cursor.execute(queries[2])
        self.db_connection.commit()

    def _create_labeled_user_set(self):
        path = "./dataset/dataset/labeled_ids.txt"
        with open(path, 'r') as labeled_ids:
            return list(map(lambda x: x.rstrip('\n'), labeled_ids.readlines()))

    def _get_users(self):
        path = "./dataset/dataset/Data/"
        data = []
        user_set = self._create_labeled_user_set()
        for directory in os.listdir(path):
            has_labels = directory in user_set
            data.append((directory, has_labels))
        return data

    def insert_users(self):
        users = self._get_users()

        query = "INSERT INTO users (id, has_labels) VALUES (%s, %s)"
        self.cursor.executemany(query, users)
        self.db_connection.commit()

    # 1. Gå til bruker
    # 2. Loop gjennom `trajectory/<yyyyMMddHHmmss>.plt`
    #    1. Sjekk om det finnes en aktivitet i brukerens `labels.txt` der start time korresponderer med navn på trajectory-fil
    #       1. Hvis det korresponderer, bruk `labels.txt`-oppføringen til å lage activity'en, før alle punkter legges inn i den opprettede aktiviteten
    #       2. Hvis det ikke korresponderer, bruk første og siste punkt i trajectory til å lage en activity for alle punkter så legges inn

    def _insert_activity_query(self, id, transportation_mode, start_date_time, end_date_time, user_id):
        query = """INSERT INTO activities (id, transportation_mode, start_date_time, end_date_time, user_id) 
                    VALUES ('%s', '%s', '%s', '%s', '%s')"""

        return query % (id, transportation_mode, start_date_time, end_date_time, user_id)

    def _insert_track_point_query(self):
        return """INSERT INTO track_points (lat, lon, altitude, date_time, activity_id) 
                    VALUES (%s, %s, %s, %s, %s)"""

    def create_activity(self, user, file_path):
        activityId = f"{file_path}_{user[0]}"
        with open(f"./dataset/dataset/Data/{user[0]}/Trajectory/{file_path}", 'r') as trajectory_file:
            trajectory_lines = trajectory_file.readlines()

            # Don't add activities with more then 2500 trajections (2506 because the first 6 lines contains other information)
            if len(trajectory_lines) > 2506:
                return

            activityQuery = None
            first_trajectory = Trajectory(trajectory_lines[6])
            last_trajectory = Trajectory(trajectory_lines[-1])
            # If there exists a labels.txt in dir
            if user[1]:
                with open(f"./dataset/dataset/Data/{user[0]}/labels.txt", 'r') as labels_file:
                    # For all lines in label file, compare to trajectory date, insert if match
                    for label_line in labels_file.readlines()[1:]:
                        labels_activity = LabelsActivity(label_line)
                        if labels_activity.start_date == first_trajectory.date and labels_activity.end_date == last_trajectory.date:
                            activityQuery = self._insert_activity_query(
                                activityId, labels_activity.transportation_mode, labels_activity.start_date, labels_activity.end_date, user[0])
                            continue

            if not activityQuery:
                activityQuery = self._insert_activity_query(
                    activityId, None, first_trajectory.date, last_trajectory.date, user[0])

            self.cursor.execute(activityQuery)

            track_point_data = self.get_track_points_from_file(
                trajectory_lines, activityId)
            self.cursor.executemany(
                self._insert_track_point_query(), track_point_data)

    def insert_user_activities(self, user):
        path = f"./dataset/dataset/Data/{user[0]}/Trajectory/"
        for user_file in os.listdir(f"{path}"):
            self.create_activity(user, user_file)

    def get_track_points_from_file(self, trajectory_lines, activity_id):
        return list(map(lambda line: Trajectory(line).to_tuple(activity_id), trajectory_lines[6:-2]))

    def insert_trajectory(self):
        count = 0
        # users = self._get_users()[81:82]
        users = self._get_users()
        for user in users:
            print(f"Start {user[0]}, count: {count}")
            self.insert_user_activities(user)
            self.db_connection.commit()
            count += 1
            print(f"Finished {user[0]}, count: {count}")

    def _run_query(self, query: str):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    # Task 2.1

    def task_2_1_count_tables_sizes(self):
        tables = [('users',), ("activities",), ("track_points",)]
        query = """
                SELECT COUNT(*) FROM %s
                """
        print("Task 2.1")
        for table in tables:

            self.cursor.execute(query % table)
            rows = self.cursor.fetchall()
            print(tabulate(rows, headers=table))

    # Task 2.2

    def task_2_2_average_activities_per_user(self):
        query = """
                select (select count(*) from activities) / (select count(*) from users) AS divide from dual;
                """

        print("Task 2.2")
        self._run_query(query)

    # Task 2.3
    def task_2_3_top_20_users_with_most_activities(self):
        query = """
                SELECT u.id, COUNT(*) as num_of_activities
                FROM users u
                LEFT JOIN activities a ON (u.id = a.user_id)
                GROUP BY u.id
                ORDER BY num_of_activities DESC
                LIMIT 20
                """
        print("Task 2.3")
        self._run_query(query)

    # Task 2.4
    def task_2_4_users_whos_taken_taxi(self):
        query = """
                SELECT DISTINCT u.id from users u 
                JOIN activities a ON (u.id = a.user_id AND a.transportation_mode = 'taxi')
                """

        print("Task 2.4")
        self._run_query(query)

    # Task 2.5
    def task_2_5_count_transportations(self):
        query = """
                SELECT transportation_mode, COUNT(*) as transportation_count
                FROM activities
                WHERE transportation_mode != 'NONE'
                GROUP BY transportation_mode
                ORDER BY transportation_count DESC
                """

        print("Task 2.5")
        self._run_query(query)

    # Task 2.6 a)
    def task_2_6_a_year_with_most_activities(self):
        query = """
                SELECT YEAR(start_date_time) as start_year, COUNT(*) as year_count
                FROM activities
                GROUP BY start_year
                ORDER BY year_count DESC
                """

        print("Task 2.6 a)")
        self._run_query(query)

    # Task 2.6 b)
    def task_2_6_b_year_with_longest_activities(self):
        query = """
                SELECT YEAR(a.start_date_time) as start_year, SUM(HOUR(TIMEDIFF(a.start_date_time, a.end_date_time))) as sum_time
                FROM activities as a
                GROUP BY start_year
                ORDER BY sum_time DESC
                """

        print("Task 2.6 b)")
        self._run_query(query)

    # Task 2.7
    def task_2_7_total_distance_walked_in_2008_by_112(self):
        def calculate_distance(positions: list[tuple[float, float]]):
            """
            Calculates distance in km through a list of geo-coordinates using the Haversine-function
            Found at: https://stackoverflow.com/a/41438745
            """
            results = []
            for i in range(1, len(positions)):
                loc1 = positions[i - 1]
                loc2 = positions[i]

                lat1 = loc1[0]
                lng1 = loc1[1]

                lat2 = loc2[0]
                lng2 = loc2[1]

                degreesToRadians = (math.pi / 180)
                latrad1 = lat1 * degreesToRadians
                latrad2 = lat2 * degreesToRadians
                dlat = (lat2 - lat1) * degreesToRadians
                dlng = (lng2 - lng1) * degreesToRadians

                a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(latrad1) * \
                    math.cos(latrad2) * math.sin(dlng / 2) * math.sin(dlng / 2)
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                r = 6371000

                results.append(r * c)

            return (sum(results) / 1000)  # Converting from m to km

        query = """
                SELECT t.id, t.lat, t.lon, t.activity_id, a.user_id
                FROM track_points t
                INNER JOIN activities a
                ON (
                  a.id = t.activity_id
                  AND a.transportation_mode = 'walk'
                  AND a.user_id = '112'
                  AND t.date_time between '2008-01-01 00:00:00' and '2008-12-31 23:59:59'
                );
                """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        activities = groupby(rows, lambda row: row[3])

        total_distance = 0
        for activity in activities:
            points = activity[1]
            total_distance += calculate_distance(list(map(lambda point: (point[1], point[2]), points)))

        print("Task 2.7")
        print(f"Total distance by user 112 in 2008: {total_distance} km")

    def task_2_8_top_20_users_with_most_altitude_meters(self):      
        query = """
                SELECT a.user_id, t.activity_id, t.altitude  * 0.3048, t.date_time FROM track_points t 
                JOIN activities a 
                ON t.activity_id = a.id
                WHERE t.altitude != -777
                ORDER BY a.user_id, t.activity_id, t.date_time
                """

        print("Task 2.8")
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        usersAltitudes = {}
        for i in range(len(rows) - 1):
            trackPoint = rows[i]
            nextTrackPoint = rows[i + 1]
            sameActivityId = trackPoint[1] == nextTrackPoint[1]
            sameUser = trackPoint[0] == nextTrackPoint[0]
            if sameActivityId and sameUser:
                nextTrackPointHasHigherAltitude = trackPoint[2] < nextTrackPoint[2]
                if nextTrackPointHasHigherAltitude:
                    try:
                        usersAltitudes[trackPoint[0]] += (nextTrackPoint[2] - trackPoint[2])
                    except:
                        usersAltitudes[trackPoint[0]] = (nextTrackPoint[2] - trackPoint[2])

        results = sorted(usersAltitudes.items(), key=lambda item: item[1], reverse=True)

        print(tabulate(results[:20], headers=("UserId", "Altitude meters gained")))

    def task_2_9_invalid_activities_per_user(self):
        print("Task 2.9")
        query = """
                SELECT t.activity_id, t.date_time, a.user_id 
                FROM track_points t JOIN activities a 
                ON t.activity_id = a.id 
                ORDER BY a.user_id, t.activity_id, t.date_time
                """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        invalidActivities = set()
        usersInvalidActivities = {}
        for i in range(len(rows) - 1):
            trackPoint = rows[i]
            if trackPoint[0] in invalidActivities:
                # This track-point's activity have already been marked as invalid
                continue
            nextTrackPoint = rows[i + 1]
            sameActivityId = trackPoint[0] == nextTrackPoint[0]
            sameUser = trackPoint[2] == nextTrackPoint[2]
            if sameActivityId and sameUser:
                trackpointsDiffInSeconds = (nextTrackPoint[1] - trackPoint[1]).seconds
                if trackpointsDiffInSeconds >= 5 * 60: # 5 minutes
                    invalidActivities.add(trackPoint[0])
                    if trackPoint[2] in usersInvalidActivities:
                        usersInvalidActivities[trackPoint[2]] += 1
                    else:
                        usersInvalidActivities[trackPoint[2]] = 1

        results = sorted(usersInvalidActivities.items(), key=lambda x: x[1], reverse=True)

        print(tabulate(results[:20], headers=("UserId", "Invalid activities")))

    def task_2_10_users_tracked_forbidden_city(self):
        print("Task 2.10")
        query = """
                SELECT DISTINCT u.id
                FROM users u
                LEFT JOIN activities a ON (u.id = a.user_id)
                LEFT JOIN track_points t ON (a.id = t.activity_id)
                WHERE t.lat BETWEEN 39.916 AND 39.917 AND t.lon BETWEEN 116.397 AND 116.398
                """
        self._run_query(query)

    def task_2_11_users_most_used_transportation_mode(self):
        print("Task 2.11")
        query = """
                SELECT u.id, COUNT(a.user_id)
                FROM users u
                LEFT JOIN (
                  SELECT a.user_id, a.transportation_mode
                  FROM activities a
                  WHERE a.transportation_mode != 'None'
                  GROUP BY a.user_id, a.transportation_mode
                  HAVING COUNT(a.transportation_mode) >= ALL(
                    SELECT COUNT(*) FROM activities a2 WHERE a2.user_id = a.user_id GROUP BY a2.transportation_mode
                  )
                ) as a on a.user_id = u.id
                """
        self._run_query(query)