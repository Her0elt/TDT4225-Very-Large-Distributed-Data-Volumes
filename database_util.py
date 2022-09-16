from DbConnector import DbConnector
from tabulate import tabulate
import uuid
import os

from util import Trajectory, LabelsActivity


class DatabaseUtil:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        query = """CREATE TABLE IF NOT EXISTS users (
                   id VARCHAR(225) NOT NULL PRIMARY KEY,
                   has_labels BOOL NOT NULL DEFAULT false
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
        return {user_id for user_id in open(path, 'r')}
            
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
    def _insert_activity_query(self, id, transportation_mode, start_date_time, end_date_time, user_id ):
        query = """INSERT INTO activities (id, transportation_mode, start_date_time, end_date_time, user_id) 
                    VALUES ('%s', '%s', '%s', '%s', '%s')"""
        
        return query % (id, transportation_mode, start_date_time, end_date_time, user_id)

    def _insert_track_point_query(self):
        return """INSERT INTO track_points (lat, lon, altitude, date_time, activity_id) 
                    VALUES (%s, %s, %s, %s, %s)"""


    def create_activity(self, user, file_path):
        activityId = f"{file_path}_{user[0]}"
        with open(f"./dataset/dataset/Data/{user[0]}/Trajectory/{file_path}", 'r') as trajectory_file:
            # Skip first 6 lines as they contain useless information
            # for _ in range(6):
            #     next(trajectory_file)
            trajectory_lines = trajectory_file.readlines()

            # Don't add activities with more then 2500 trajections
            if len(trajectory_lines) > 2506:
                return

            activityQuery = None
            first_trajectory = Trajectory(trajectory_lines[6])
            # If there exists a labels.txt in dir
            if user[1]:
                with open(f"./dataset/dataset/Data/{user[0]}/labels.txt", 'r') as labels_file:
                    activityId = uuid.uuid4()
                    # For all lines in label file, compare to trajectory date, insert if match
                    for label_line in labels_file:
                        labels_activity = LabelsActivity(label_line)
                        if labels_activity.start_date == first_trajectory.date:
                            activityQuery = self._insert_activity_query(activityId, labels_activity.transportation_mode, labels_activity.start_date, labels_activity.end_date, user[0])
                            continue

            if not activityQuery:
                last_trajectory = Trajectory(trajectory_lines[-1])
                activityQuery = self._insert_activity_query(activityId, None, first_trajectory.date, last_trajectory.date, user[0])
            
            self.cursor.execute(activityQuery)

            track_point_data = self.get_track_points_from_file(trajectory_lines, activityId)
            self.cursor.executemany(self._insert_track_point_query(), track_point_data)
            
                

    
    def insert_user_activities(self, user):
        path = f"./dataset/dataset/Data/{user[0]}/Trajectory/"
        for user_file in os.listdir(f"{path}"):
            self.create_activity(user, user_file)
            
        
    def get_track_points_from_file(self, trajectory_lines, activity_id):
        # data = list(map(lambda line: Trajectory(line).to_tuple(activity_id), trajectory_file))
        data = []
        for line in trajectory_lines[6:-2]:
            trajectory = Trajectory(line)
            data.append(trajectory.to_tuple(activity_id))
        return data        


            #for line in (trajectory_file):
                # If the user has labels
                # print(line)
                #tracjectory = Trajectory(line)


                #if 'str' in line:
                    #break
    def insert_trajectory(self): 

        users = self._get_users()
        for user in users:
            print(f"Start {user[0]}")
            self.insert_user_activities(user)
            self.db_connection.commit()
            print(f"Finished {user[0]}")
            
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
        for table in tables:
            
            self.cursor.execute(query % table)
            rows = self.cursor.fetchall()
            print(tabulate(rows, headers=table))

            
    # Task 2.2
    def task_2_2_average_activities_per_user(self):
        query = """
                select (select count(*) from activities) / (select count(*) from users) AS divide from dual;
                """
        self._run_query(query)
        
    # Task 2.3
    def task_2_3_top_20_users_with_most_activities(self):
        query = """
                SELECT users.*, count(activities.user_id) as num_of_activities
                FROM users
                LEFT JOIN activities ON (users.id = activities.user_id)
                GROUP BY activities.user_id
                """

        self._run_query(query)

        # self.cursor.execute(query)
        # rows = self.cursor.fetchall()
        # print(tabulate(rows, headers=self.cursor.column_names))
        
    # Task 2.4
    def task_2_4_users_whos_taken_taxi(self):
        query = """
                SELECT u.id from users u 
                join activities a on(u.id = a.user_id) 
                WHERE a.transportation_mode = 'taxi'
                """

        
        self._run_query(query)
   # Task 2.5
         
        

    # def insert_data(self, table_name):
    #     names = ['Bobby', 'Mc', 'McSmack', 'Board']
    #     for name in names:
    #         # Take note that the name is wrapped in '' --> '%s' because it is a string,
    #         # while an int would be %s etc
    #         query = "INSERT INTO %s (name) VALUES ('%s')"
    #         self.cursor.execute(query % (table_name, name))
    #     self.db_connection.commit()

    # def fetch_data(self, table_name):
    #     query = "SELECT * FROM %s"
    #     self.cursor.execute(query % table_name)
    #     rows = self.cursor.fetchall()
    #     print("Data from table %s, raw format:" % table_name)
    #     print(rows)
    #     # Using tabulate to show the table in a nice way
    #     print("Data from table %s, tabulated:" % table_name)
    #     print(tabulate(rows, headers=self.cursor.column_names))
    #     return rows

    # def drop_table(self, table_name):
    #     print("Dropping table %s..." % table_name)
    #     query = "DROP TABLE %s"
    #     self.cursor.execute(query % table_name)

    # def show_tables(self):
    #     self.cursor.execute("SHOW TABLES")
    #     rows = self.cursor.fetchall()
    #     print(tabulate(rows, headers=self.cursor.column_names))
    #     self.cursor.close()



